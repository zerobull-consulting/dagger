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
// length plus a header, and each varint, fixed or group field its wire size,
// tag included, or the header if that is more. A group the message doesn't
// know, like any field of the wrong wire type for its number, is kept by
// proto.Unmarshal as raw bytes, so it is charged at least its size.
const (
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
			size += max(int64(protowire.SizeTag(num)+n), otlpFieldCost)
			continue
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case fd != nil && fd.Message() != nil:
			sub, err := otlpProtoSize(v, fd.Message(), depth+1)
			if err != nil {
				return 0, err
			}
			size += sub + otlpListElemCost
		case fd != nil && fd.IsList():
			// Packed scalars: each takes at least a byte on the wire, and
			// at most 8 decoded, plus the slice's growth.
			size += int64(len(v)) * otlpListElemCost
		default:
			// A string or bytes field, copied; or a field the message
			// doesn't know, kept as raw bytes.
			size += int64(len(v)) + otlpFieldCost
		}
	}
	return size, nil
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
