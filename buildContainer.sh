../buildkit/bin/buildctl build \
    --frontend=dockerfile.v0 \
    --local context=. \
    --local dockerfile=. \
    --output type=image,name=docker.io/luoxiao23333/scheduler:v3
