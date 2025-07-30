package types

import (
	"github.com/tinylib/msgp/msgp"
	"github.com/valyala/fastjson"
)

var parserPool fastjson.ParserPool

// Oddly, while one of messagepack's selling points is its alleged interoperability
// with JSON, none of the golang messagepack libaries actually provide a method to
// do the translation in this direction. So here's a simple implementation which
// is fast but doesn't go to any great lengths to re-create types which were lost
// during JSON serialization; for example, dates that were encoded as strings stay
// strings, where a direct msgp encoding would have used the time type. This is
// fine for our field payload maps but doesn't work well with structs.
func JSONToMessagePack(buf []byte, jsonData []byte) ([]byte, error) {
	parser := parserPool.Get()
	defer parserPool.Put(parser)

	v, err := parser.ParseBytes(jsonData)
	if err != nil {
		return buf, err
	}

	buf, err = appendJSONValue(buf, v)
	if err != nil {
		return buf, err
	}

	return buf, nil
}

func appendJSONValue(buf []byte, v *fastjson.Value) ([]byte, error) {
	switch v.Type() {
	case fastjson.TypeObject:
		return appendJSONObject(buf, v)
	case fastjson.TypeArray:
		return appendJSONArray(buf, v)
	case fastjson.TypeString:
		s, _ := v.StringBytes()
		return msgp.AppendString(buf, string(s)), nil
	case fastjson.TypeNumber:
		if float64(v.GetInt64()) == v.GetFloat64() {
			return msgp.AppendInt64(buf, v.GetInt64()), nil
		}
		return msgp.AppendFloat64(buf, v.GetFloat64()), nil
	case fastjson.TypeTrue:
		return msgp.AppendBool(buf, true), nil
	case fastjson.TypeFalse:
		return msgp.AppendBool(buf, false), nil
	case fastjson.TypeNull:
		return msgp.AppendNil(buf), nil
	default:
		return msgp.AppendIntf(buf, nil)
	}
}

func appendJSONObject(buf []byte, v *fastjson.Value) ([]byte, error) {
	obj, err := v.Object()
	if err != nil {
		return buf, err
	}

	buf = msgp.AppendMapHeader(buf, uint32(obj.Len()))

	obj.Visit(func(key []byte, v *fastjson.Value) {
		if err != nil {
			return
		}
		buf = msgp.AppendStringFromBytes(buf, key)
		buf, err = appendJSONValue(buf, v)
	})

	return buf, err
}

func appendJSONArray(buf []byte, v *fastjson.Value) ([]byte, error) {
	arr, err := v.Array()
	if err != nil {
		return buf, err
	}

	buf = msgp.AppendArrayHeader(buf, uint32(len(arr)))

	for _, item := range arr {
		buf, err = appendJSONValue(buf, item)
		if err != nil {
			return buf, err
		}
	}

	return buf, nil
}
