package v1alpha

//go:generate bash -c "../../../hack/protoc.sh --plugin=protoc-gen-go=$(go tool -n protoc-gen-go) --plugin=protoc-gen-go-grpc=$(go tool -n protoc-gen-go-grpc) --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative server.proto"
