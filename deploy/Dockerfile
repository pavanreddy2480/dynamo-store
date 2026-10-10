# Built from prebuilt static binaries (CGO_ENABLED=0), so no base image is
# needed: the container holds only the two programs.
FROM scratch
COPY --chmod=0755 dynode dyload /
ENTRYPOINT ["/dynode"]
