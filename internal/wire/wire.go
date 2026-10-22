package wire

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"math"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const MaxMessage = 2 << 20
const MaxObject = 1 << 20

type Handler interface {
	Call(context.Context, *structpb.Struct) (*structpb.Struct, error)
}
type Service func(context.Context, map[string]any) (any, error)

func (s Service) Call(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
	if !finiteStruct(in) {
		return nil, status.Error(codes.InvalidArgument, "non-finite protobuf number")
	}
	out, err := s(ctx, in.AsMap())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return Message(out)
}
func finiteStruct(in *structpb.Struct) bool {
	for _, value := range in.Fields {
		if !finiteValue(value) {
			return false
		}
	}
	return true
}
func finiteValue(value *structpb.Value) bool {
	switch v := value.Kind.(type) {
	case *structpb.Value_NumberValue:
		return !math.IsNaN(v.NumberValue) && !math.IsInf(v.NumberValue, 0)
	case *structpb.Value_StructValue:
		return finiteStruct(v.StructValue)
	case *structpb.Value_ListValue:
		for _, child := range v.ListValue.Values {
			if !finiteValue(child) {
				return false
			}
		}
	}
	return true
}
func Message(v any) (*structpb.Struct, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err = json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return structpb.NewStruct(m)
}
func Decode(m map[string]any, v any) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
func Loopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	return err == nil && ip != nil && ip.IsLoopback()
}
func Serve(ctx context.Context, address, token string, service Service) error {
	if !Loopback(address) || len(token) < 24 {
		return errors.New("loopback address and capability token of at least 24 characters required")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(MaxMessage), grpc.MaxSendMsgSize(MaxMessage), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid capability")
		}
		return next(ctx, req)
	}))
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "runtime.Runtime", HandlerType: (*Handler)(nil), Methods: []grpc.MethodDesc{{MethodName: "Call", Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		in := new(structpb.Struct)
		if err := dec(in); err != nil {
			return nil, err
		}
		handler := func(ctx context.Context, req any) (any, error) {
			return srv.(Handler).Call(ctx, req.(*structpb.Struct))
		}
		if interceptor == nil {
			return handler(ctx, in)
		}
		return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/runtime.Runtime/Call"}, handler)
	}}}}, service)
	go func() { <-ctx.Done(); server.Stop() }()
	return server.Serve(listener)
}
func Call(ctx context.Context, address, token string, request any, result any) error {
	if !Loopback(address) {
		return errors.New("non-loopback peer rejected")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessage), grpc.MaxCallSendMsgSize(MaxMessage)))
	if err != nil {
		return err
	}
	defer conn.Close()
	in, err := Message(request)
	if err != nil {
		return err
	}
	out := new(structpb.Struct)
	err = conn.Invoke(metadata.AppendToOutgoingContext(ctx, "authorization", token), "/runtime.Runtime/Call", in, out)
	if err != nil {
		return err
	}
	return Decode(out.AsMap(), result)
}
