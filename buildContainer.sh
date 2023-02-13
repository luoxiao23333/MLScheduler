../buildkit/bin/buildctl build \
    --frontend=dockerfile.v0 \
    --local context=. \
    --local dockerfile=. \
    --output type=image,name=scheduler:v0,namespace=RM
