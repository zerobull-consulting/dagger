package server

import (
	"bytes"
	"errors"
	"reflect"
	"sync"
	"unsafe"

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
// (its pointer, and the slice's growth), each singular string or bytes field
// its length and a quarter (size classes: otlpAllocSize) plus a header, each
// element of a repeated string or bytes field its length and a quarter plus
// four of its headers (its header in the slice, and the slice's growth), and
// each known varint or fixed field its wire size, tag included, or the header
// if that is more. A field the message doesn't
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
	// otlpElemHeaders is how many of its headers an element of a repeated
	// string or bytes field is charged: its header in the field's slice,
	// which append holds twice over while it grows (the old array and the
	// new one, about twice the old, and more once rounded up to a size
	// class). The most held per element, by TestOTLPAppendPeakHeaders, is
	// 3.12 string headers (at 33 elements, 32 grown to 71) and 3.25 slice
	// headers (at 75) (zerobull-consulting/dagger#217).
	otlpElemHeaders = 4
	// otlpBytesHeader is a bytes value's header, a slice's; a string's is
	// otlpFieldCost.
	otlpBytesHeader = int64(unsafe.Sizeof([]byte(nil)))
	// otlpMaxDepth bounds how deep a body may nest messages, the request
	// being depth 0; a deeper body is refused with 400 before the estimate
	// walks it or proto.Unmarshal decodes it, since both recurse once per
	// level. OTLP's own messages nest 8 deep down to an AnyValue (see
	// TestOTLPFixedNestingDepth), and the e2e checks that real engines'
	// requests stay well under this (zerobull-consulting/dagger#86).
	otlpMaxDepth = 64
	// otlpMaxJSONDepth bounds how deep a JSON body may nest objects and
	// arrays. A message in a repeated field takes an array and an object, so
	// it is twice otlpMaxDepth.
	otlpMaxJSONDepth = 2 * otlpMaxDepth
	// otlpDecodeStack is the stack the decode takes at the deepest body the
	// limits allow, reserved from the budget with the message: the walk and
	// proto.Unmarshal each recurse once per level (measured at 64 levels: the
	// walk needs 16 to 32 KiB, Unmarshal 32 to 64 KiB, as goroutine stacks
	// grow by doubling, in zerobull-consulting/dagger#86).
	otlpDecodeStack = 64 << 10
	// otlpJSONDecodeStack is the same for a JSON body, which protojson decodes
	// over up to otlpMaxJSONDepth objects and arrays: measured at 120 levels,
	// it needs 64 to 128 KiB.
	otlpJSONDecodeStack = 128 << 10
	// otlpJSONObjectCost is what one JSON object is estimated to decode to:
	// the largest OTLP message (a Span, 280 bytes) and its slice element,
	// rounded up.
	otlpJSONObjectCost = 320
)

// MaxOTLPNesting is otlpMaxDepth: the most levels of messages a protobuf body
// may nest (a JSON body, objects and arrays twice that), for the e2e to check
// real engines against.
const MaxOTLPNesting = otlpMaxDepth

var errOTLPDepth = errors.New("otlp message nested too deep")

// errOTLPEndGroup is an end-group tag that doesn't close the group open.
var errOTLPEndGroup = errors.New("otlp group closed by a mismatched end-group tag")

// otlpDecodedSize estimates the heap msg takes once decoded from data, as
// protobuf, or as JSON when asJSON. A protobuf body that isn't a valid
// encoding is an error, as decoding it would be.
func otlpDecodedSize(data []byte, msg proto.Message, asJSON bool) (int64, error) {
	size, _, err := otlpDecodedSizeDepth(data, msg, asJSON)
	return size, err
}

// otlpDecodedSizeDepth is otlpDecodedSize, and also says how deep the body
// nests messages (0 for a request with none), or -1 for JSON, whose depth is
// bounded in objects and arrays, not messages, and isn't measured.
func otlpDecodedSizeDepth(data []byte, msg proto.Message, asJSON bool) (size int64, depth int, err error) {
	if asJSON {
		if otlpJSONDepth(data) > otlpMaxJSONDepth {
			return 0, -1, errOTLPDepth
		}
		return otlpJSONSize(data), -1, nil
	}
	size, err = otlpProtoSize(data, msg.ProtoReflect().Descriptor(), 0, &depth)
	return size, depth, err
}

func otlpProtoSize(b []byte, md protoreflect.MessageDescriptor, depth int, deepest *int) (int64, error) {
	if depth > otlpMaxDepth {
		return 0, errOTLPDepth
	}
	*deepest = max(*deepest, depth)
	size := otlpMessageCost(md)
	fields := md.Fields()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		// The decoder keeps an unknown field byte for byte, so charge the
		// bytes consumed, not the canonical encoding: a varint can be padded.
		tagLen := int64(n)
		b = b[n:]
		fd := fields.ByNumber(num)
		if typ != protowire.BytesType {
			// A varint or fixed field is small, but a start-group runs to its
			// end-group, and proto.Unmarshal keeps a group the message doesn't
			// know (and any field of the wrong wire type for its number) as raw
			// bytes. So charge every such field at least its wire size: the
			// flat cost for the small ones, the size for a group.
			if typ == protowire.StartGroupType {
				// Groups nest, and ConsumeFieldValue recurses into them (to
				// its own limit, 10,000): walk them here, counting their
				// levels toward otlpMaxDepth.
				var err error
				if n, err = otlpGroupLen(b, num, depth, deepest); err != nil {
					return 0, err
				}
			} else if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return 0, protowire.ParseError(n)
			}
			b = b[n:]
			wire := tagLen + int64(n)
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
			size += otlpUnknownCost(tagLen + int64(n))
		case fd.Message() != nil:
			sub, err := otlpProtoSize(v, fd.Message(), depth+1, deepest)
			if err != nil {
				return 0, err
			}
			size += sub + otlpListElemCost
		case fd.IsList() && (fd.Kind() == protoreflect.StringKind || fd.Kind() == protoreflect.BytesKind):
			// An element of a repeated string or bytes field: copied, and its
			// header appended to the field's slice, which append grows.
			header := int64(otlpFieldCost)
			if fd.Kind() == protoreflect.BytesKind {
				header = otlpBytesHeader
			}
			size += otlpAllocSize(int64(len(v))) + otlpElemHeaders*header
		case fd.IsList():
			// Packed scalars: each takes at least a byte on the wire, and
			// at most 8 decoded, plus the slice's growth.
			size += int64(len(v)) * otlpListElemCost
		default:
			// A singular string or bytes field (a trace or span ID is one),
			// copied: charged for its size class (otlpAllocSize), and a header
			// on top, which is slack at small sizes, where a quarter is less
			// than the class (17 bytes take 24, not 21). An empty one is
			// charged the header only (zerobull-consulting/dagger#232).
			size += otlpAllocSize(int64(len(v))) + otlpFieldCost
		}
	}
	return size, nil
}

// otlpGroupLen is the length of the group that starts at the front of b,
// its end-group tag included: the group num, in a message at depth. It walks
// the group's nested groups with a stack of at most otlpMaxDepth entries, not
// by recursion, and refuses one that nests past otlpMaxDepth (each level a
// message's depth would take).
func otlpGroupLen(b []byte, num protowire.Number, depth int, deepest *int) (int, error) {
	open := []protowire.Number{num}
	off := 0
	for len(open) > 0 {
		if depth+len(open) > otlpMaxDepth {
			return 0, errOTLPDepth
		}
		*deepest = max(*deepest, depth+len(open))
		tag, typ, n := protowire.ConsumeTag(b[off:])
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		off += n
		switch typ {
		case protowire.StartGroupType:
			open = append(open, tag)
		case protowire.EndGroupType:
			if tag != open[len(open)-1] {
				return 0, errOTLPEndGroup
			}
			open = open[:len(open)-1]
		default:
			if n = protowire.ConsumeFieldValue(tag, typ, b[off:]); n < 0 {
				return 0, protowire.ParseError(n)
			}
			off += n
		}
	}
	return off, nil
}

// otlpJSONDepth is how deep a JSON body nests objects and arrays, counting
// only the braces and brackets outside strings (a string's quotes end at the
// first quote not escaped by a backslash). It stops once the depth is past
// otlpMaxJSONDepth. A body that isn't valid JSON gets whatever depth the scan
// finds; the decode refuses it.
func otlpJSONDepth(data []byte) int {
	depth, deepest := 0, 0
	inString, escaped := false, false
	for _, c := range data {
		switch {
		case escaped:
			escaped = false
		case inString:
			switch c {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
		case c == '"':
			inString = true
		case c == '{' || c == '[':
			depth++
			if deepest = max(deepest, depth); deepest > otlpMaxJSONDepth {
				return deepest
			}
		case c == '}' || c == ']':
			depth = max(depth-1, 0)
		}
	}
	return deepest
}

// otlpBytesKind says whether a known field is read from a length-delimited
// value: a message, a string, bytes, or a packed list.
func otlpBytesKind(fd protoreflect.FieldDescriptor) bool {
	return fd.IsList() || fd.Message() != nil || fd.Kind() == protoreflect.StringKind || fd.Kind() == protoreflect.BytesKind
}

// otlpAllocSize is at least what the allocator gives a copy of n bytes,
// which it rounds up to a size class, or to 8 KiB pages past 32 KiB: by up
// to a quarter (a 32,769-byte string takes 40,960), and to 16 bytes below
// that (17 bytes take 24, 33 take 48), which the headers charged with each
// copy cover.
func otlpAllocSize(n int64) int64 {
	return n + n/4
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
