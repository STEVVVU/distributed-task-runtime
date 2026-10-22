# Dependency and design provenance

Inspected 2026-10-21. Runtime code in this checkout is independently authored. The student Baby Ray source was not copied because a reusable license was not verified. Its paper informs the component split, futures and failure-recovery goals; the implementation deliberately uses a single durable coordinator and pull-based global placement rather than reproducing every Ray scheduling policy.

| Dependency | Pinned version | Primary source and license | Compatibility/evidence |
| --- | --- | --- | --- |
| gRPC Go | v1.84.0 | [grpc/grpc-go](https://github.com/grpc/grpc-go), [Apache-2.0](https://github.com/grpc/grpc-go/blob/v1.84.0/LICENSE) | Official Go gRPC server/client and protobuf codec; real Go/Python unary RPC driver passes. |
| bbolt | v1.5.0 | [etcd-io/bbolt](https://github.com/etcd-io/bbolt), [MIT](https://github.com/etcd-io/bbolt/blob/v1.5.0/LICENSE) | Official embedded transaction store; Windows reopen tests and killed-coordinator recovery pass. Default durable transactions, no NoSync option. |
| Go protobuf | v1.36.11 | [protocolbuffers/protobuf-go](https://github.com/protocolbuffers/protobuf-go), [BSD-3-Clause](https://github.com/protocolbuffers/protobuf-go/blob/v1.36.11/LICENSE) | Standard Struct wire type, interoperable with Python protobuf. |
| Python grpcio | 1.84.0 | [grpc/grpc](https://github.com/grpc/grpc), [Apache-2.0](https://github.com/grpc/grpc/blob/v1.84.0/LICENSE) | Official Windows CPython 3.11 wheel from PyPI; real RPCs pass. |
| Python protobuf | 7.36.2 | [protocolbuffers/protobuf](https://github.com/protocolbuffers/protobuf), [BSD-3-Clause](https://github.com/protocolbuffers/protobuf/blob/main/LICENSE) | Official PyPI wheel; standard Struct serializer/parser. |
| typing_extensions | 4.16.0 | [python/typing_extensions](https://github.com/python/typing_extensions), [PSF-2.0](https://github.com/python/typing_extensions/blob/main/LICENSE) | grpcio dependency, pinned in the Python lock. |

The full Go graph is pinned by `go.mod` and `go.sum`. Current gRPC's initial transitive `golang.org/x/net v0.57.0` scan reported reachable GO-2026-6617, GO-2026-6612, GO-2026-6611 and GO-2026-6603. The graph was upgraded to x/net v0.61.0, x/sys v0.49.0 and x/text v0.43.0 using official module sources. `govulncheck@v1.9.0` then reported no vulnerabilities. `pip-audit==2.10.1 -r requirements.lock` reported no known vulnerabilities. These scans describe their databases on the validation date, not a permanent security guarantee.

No third-party source is vendored. Downloaded package license notices remain in their installed module/wheel distributions. The application MIT license does not relicense its dependencies or the cited papers.

CI uses official MIT-licensed GitHub actions, inspected at immutable commits on 2026-10-21: [checkout](https://github.com/actions/checkout/tree/d23441a48e516b6c34aea4fa41551a30e30af803), [setup-go](https://github.com/actions/setup-go/tree/924ae3a1cded613372ab5595356fb5720e22ba16), [setup-python](https://github.com/actions/setup-python/tree/ece7cb06caefa5fff74198d8649806c4678c61a1) and [upload-artifact](https://github.com/actions/upload-artifact/tree/b7c566a772e6b6bfb58ed0dc250532a479d7789f). The workflow targets the locally validated Windows/Go 1.27.3/Python 3.11 combination. Its static checker is [actionlint v1.7.11](https://github.com/rhysd/actionlint/tree/v1.7.11), MIT licensed and run from the isolated Go module cache without adding a runtime dependency.
