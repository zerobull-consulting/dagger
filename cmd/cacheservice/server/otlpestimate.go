package server

import (
	"bytes"
	"errors"
	"reflect"
	"sync"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// What decoding an OTLP body costs on the heap, estimated from the body
// before it is decoded, so the decode budget can count the message as well as
// the bytes it comes from. A body can decode to far more than its size: an
// empty ResourceSpans takes 2 bytes on the wire and 88 in memory, plus its
// pointer, so a body of them decodes to about 70 times its size. The
// estimate errs high: each message at its Go struct's size plus an eighth
// (allocation size classes), each element of a repeated field 16 bytes more
// (its pointer, and the slice's growth), each string or bytes field its
// length plus a header, and each known varint or fixed field its wire size,
// tag included, or the header if that is more. A field the message doesn't
// know (a group, a bytes field, like any field of the wrong wire type for
// its number) is kept by proto.Unmarshal as raw bytes, appended to one slice
// per message, so it is charged otlpUnknownGrowth times its wire size (or
// the header if that is more), for the old array and the new one that append
// holds at once while it grows. The estimate is a peak: what the decode
// holds while it runs, which is what the decode budget reserves until the
// request ends.
const (
	// otlpUnknownGrowth multiplies the wire size of an unknown field. Appending
	// to a slice that must grow holds the old array and a new one, about 1.25
	// times as big, so peak live heap is up to about 2.25 times the bytes kept,
	// and more with size-class rounding. Measured in
	// zerobull-consulting/dagger#204: 1.5 to 1.6 times the wire bytes sampled
	// at GC, 2.26 times at the worst growth step for 64 KiB fields.
	otlpUnknownGrowth = 3
	// otlpListElemCost is what one element of a repeated field adds to its
	// slice: a pointer or a scalar, and room for the slice's doubling.
	otlpListElemCost = 16
	// otlpFieldCost is a scalar field's or a string's header, and the least a
	// varint, fixed or group field is charged.
	otlpFieldCost = 16
	// otlpMaxDepth bounds how deep the estimate walks nested messages, as
	// proto.Unmarshal does (protowire's default recursion limit).
	otlpMaxDepth = 10000
	// otlpJSONObjectCost is what one JSON object is estimated to decode to:
	// the largest OTLP message (a Span, 280 bytes) and its slice element,
	// rounded up.
	otlpJSONObjectCost = 320
)

var errOTLPDepth = errors.New("otlp message nested too deep")

// otlpDecodedSize estimates the heap msg takes once decoded from data, as
// protobuf, or as JSON when asJSON. A protobuf body that isn't a valid
// encoding is an error, as decoding it would be.
func otlpDecodedSize(data []byte, msg proto.Message, asJSON bool) (int64, error) {
	if asJSON {
		return otlpJSONSize(data), nil
	}
	return otlpProtoSize(data, msg.ProtoReflect().Descriptor(), 0)
}

func otlpProtoSize(b []byte, md protoreflect.MessageDescriptor, depth int) (int64, error) {
	if depth > otlpMaxDepth {
		return 0, errOTLPDepth
	}
	size := otlpMessageCost(md)
	fields := md.Fields()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		b = b[n:]
		fd := fields.ByNumber(num)
		if typ != protowire.BytesType {
			// A varint or fixed field is small, but a start-group runs to its
			// end-group, and proto.Unmarshal keeps a group the message doesn't
			// know (and any field of the wrong wire type for its number) as raw
			// bytes. So charge every such field at least its wire size: the
			// flat cost for the small ones, the size for a group.
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			b = b[n:]
			wire := int64(protowire.SizeTag(num) + n)
			if fd == nil || !otlpWireTypeOK(fd, typ) {
				// Kept as unknown bytes, which append grows.
				size += otlpUnknownCost(wire)
			} else {
				size += max(wire, otlpFieldCost)
			}
			continue
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case fd == nil || !otlpBytesKind(fd):
			// A field the message doesn't know, or of the wrong wire type for
			// its number: kept as raw bytes, tag and length included, in the
			// message's unknown fields, which append grows.
			size += otlpUnknownCost(int64(protowire.SizeTag(num) + protowire.SizeBytes(len(v))))
		case fd.Message() != nil:
			sub, err := otlpProtoSize(v, fd.Message(), depth+1)
			if err != nil {
				return 0, err
			}
			size += sub + otlpListElemCost
		case fd.IsList():
			// Packed scalars: each takes at least a byte on the wire, and
			// at most 8 decoded, plus the slice's growth.
			size += int64(len(v)) * otlpListElemCost
		default:
			// A string or bytes field, copied.
			size += int64(len(v)) + otlpFieldCost
		}
	}
	return size, nil
}

// otlpBytesKind says whether a known field is read from a length-delimited
// value: a message, a string, bytes, or a packed list.
func otlpBytesKind(fd protoreflect.FieldDescriptor) bool {
	return fd.IsList() || fd.Message() != nil || fd.Kind() == protoreflect.StringKind || fd.Kind() == protoreflect.BytesKind
}

// otlpUnknownCost is what a field the decoder keeps as unknown bytes adds to
// the estimate: wire bytes, tag and length included, times otlpUnknownGrowth,
// or the header if that is more.
func otlpUnknownCost(wire int64) int64 {
	return max(otlpUnknownGrowth*wire, otlpFieldCost)
}

// otlpWireTypeOK says whether a known field of wire type typ (not bytes) is
// read into the message: a scalar of that wire type, or an element of a list
// of them. Anything else, a kind not listed below (a group, which no OTLP
// message has) included, is kept as unknown bytes: charged as unknown, so it
// can only err high.
func otlpWireTypeOK(fd protoreflect.FieldDescriptor, typ protowire.Type) bool {
	switch fd.Kind() {
	case protoreflect.BoolKind, protoreflect.EnumKind, protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Sint32Kind, protoreflect.Sint64Kind:
		return typ == protowire.VarintType
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
		return typ == protowire.Fixed32Type
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
		return typ == protowire.Fixed64Type
	}
	return false
}

// otlpJSONSize estimates what a JSON body decodes to: each object at the
// largest message's cost, each comma an element of a repeated field, and the
// body's bytes for the strings it holds. It counts braces and commas inside
// strings too, so it errs high.
func otlpJSONSize(data []byte) int64 {
	objects := int64(bytes.Count(data, []byte("{")))
	elems := int64(bytes.Count(data, []byte(",")))
	return objects*otlpJSONObjectCost + elems*otlpListElemCost + int64(len(data))
}

// otlpMessageCosts caches each message type's cost by its full name.
var otlpMessageCosts sync.Map

// otlpMessageCost is what one message of md takes on the heap: its Go
// struct's size, plus an eighth for the allocator's size classes.
func otlpMessageCost(md protoreflect.MessageDescriptor) int64 {
	if c, ok := otlpMessageCosts.Load(md.FullName()); ok {
		return c.(int64)
	}
	cost := int64(otlpJSONObjectCost)
	if mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName()); err == nil {
		if t := reflect.TypeOf(mt.Zero().Interface()); t.Kind() == reflect.Pointer {
			size := int64(t.Elem().Size())
			cost = size + size/8
		}
	}
	otlpMessageCosts.Store(md.FullName(), cost)
	return cost
}
