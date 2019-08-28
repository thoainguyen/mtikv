echo "generate go code..."
ret=0

gen() {
  base_name=$(basename $1 ".proto")

  mkdir -p ../../pkg/api/$base_name


  protoc -I/usr/local/include -I. \
    -I$GOPATH/src \
    -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
    --go_out=plugins=grpc:../../pkg/api/$base_name $1 || ret=$?

  protoc -I/usr/local/include -I. \
    -I$GOPATH/src \
    -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
    --grpc-gateway_out=logtostderr=true:../../pkg/api/$base_name  $1 || ret=$?

}

cd ../api/proto

for file in `ls *.proto`
  do
  gen $file
done

cd ../../

exit $ret


