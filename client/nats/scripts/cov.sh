#!/bin/bash -e
# Run from directory above via ./scripts/cov.sh

rm -rf ./cov
mkdir cov
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/nats.out . -tags=skip_no_race_tests
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/test.out -coverpkg=github.com/nats-io/nats.go ./test -tags=skip_no_race_tests
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/builtin.out -coverpkg=github.com/nats-io/nats.go/encoders/builtin ./test -run EncBuiltin -tags=skip_no_race_tests
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/protobuf.out -coverpkg=github.com/nats-io/nats.go/encoders/protobuf ./test -run EncProto -tags=skip_no_race_tests
gocovmerge ./cov/*.out > acc.out
rm -rf ./cov

# Without argument, launch browser results. We are going to push to coveralls only
# from Travis.yml and after success of the build (and result of pushing will not affect
# build result).
if [[ $1 == "" ]]; then
    go tool cover -html=acc.out
fi
