HAS_DEP := $(shell command -v dep;)
DEP_VERSION := v0.5.0
GIT_TAG := $(shell git describe --tags --always)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
LDFLAGS := "-X main.GitTag=${GIT_TAG} -X main.GitCommit=${GIT_COMMIT}"
DIST := $(CURDIR)/dist
DOCKER_USER := $(shell printenv DOCKER_USER)
DOCKER_PASSWORD := $(shell printenv DOCKER_PASSWORD)
TRAVIS := $(shell printenv TRAVIS)

all: bootstrap build docker push

fmt:
	go fmt -mod=mod ./pkg/... ./cmd/...

vet:
	go vet -mod=mod  ./pkg/... ./cmd/...

# Build cain binary
build: fmt vet
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=mod -ldflags $(LDFLAGS) -o bin/cain cmd/cain.go

# Build cain docker image
docker: fmt vet
	cp bin/cain cain
	docker build -t nuvo/cain:latest .
	rm cain


# Push will only happen in travis ci
push:
ifdef TRAVIS
ifdef DOCKER_USER
ifdef DOCKER_PASSWORD
	docker login -u $(DOCKER_USER) -p $(DOCKER_PASSWORD)
	docker push nuvo/cain:latest
endif
endif
endif

bootstrap:
ifndef HAS_DEP
	wget -q -O $(GOPATH)/bin/dep https://github.com/golang/dep/releases/download/$(DEP_VERSION)/dep-linux-amd64
	chmod +x $(GOPATH)/bin/dep
endif
	dep ensure

dist:
	mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags $(LDFLAGS) -o cain cmd/cain.go
	tar -zcvf $(DIST)/cain-linux-$(GIT_TAG).tgz cain
	rm cain
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags $(LDFLAGS) -o cain cmd/cain.go
	tar -zcvf $(DIST)/cain-macos-$(GIT_TAG).tgz cain
	rm cain
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags $(LDFLAGS) -o cain.exe cmd/cain.go
	tar -zcvf $(DIST)/cain-windows-$(GIT_TAG).tgz cain.exe
	rm cain.exe