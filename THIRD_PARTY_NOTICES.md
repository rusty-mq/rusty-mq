# Third-party notices

rusty-mq original code is licensed `MIT OR Apache-2.0`. The following
third-party components are reused under their own licenses; notices are
preserved here per the PRD §8.2 licensing policy. This file must be updated
whenever a dependency is added or its license verified (full transitive audit
is an M9 release gate; direct dependencies are covered at M0).

## Direct dependencies

| Crate | Version | License | Use |
| --- | --- | --- | --- |
| amq-protocol (incl. -types) | 7.2.x | BSD-2-Clause | AMQP 0-9-1 wire types/codecs (ADR-0006) |
| tokio | 1.x | MIT | async runtime |
| bytes | 1.x | MIT | buffer utilities |
| serde / toml | 1.x / 0.8 | MIT | configuration parsing |
| tracing / tracing-subscriber | 0.1 / 0.3 | MIT | structured logging |
| clap | 4.x | MIT OR Apache-2.0 | CLI |
| proptest | 1.x | MIT OR Apache-2.0 | property tests |
| lapin (dev) | 7.x | MIT OR Apache-2.0 | interop test client |
| tempfile (dev) | 3.x | MIT OR Apache-2.0 | test data dirs |

## BSD-2-Clause notice (amq-protocol)

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

## Specification inputs

AMQP 0-9-1 specification material (via the rabbitmq/amqp-0.9.1-spec
repository and amq-protocol's generated code) retains its original copyright;
no specification text is relicensed as part of rusty-mq.
