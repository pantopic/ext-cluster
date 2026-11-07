

gen: gen-lite

gen-lite:
	@mkdir -p sdk-go/pb
	@protoc cluster.proto \
		--plugin protoc-gen-go-lite="${GOBIN}/protoc-gen-go-lite" \
		--go-lite_out=sdk-go/pb  \
		--go-lite_opt=paths=source_relative \
		--go-lite_opt=features=marshal+unmarshal+size \
		-I .
	@mkdir -p host-wazero/pb
	@cp sdk-go/pb/*.go host-wazero/pb

gen-lite-install:
	go install github.com/aperturerobotics/protobuf-go-lite/cmd/protoc-gen-go-lite@latest
