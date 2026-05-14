<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Scalar UDFs

A scalar UDF is a Java-implemented SQL function that operates on one row at a
time, expressed in vectorised form: each invocation receives a batch of input
columns and returns a single output column of the same length.

## Implement

Implement the `ScalarUdf` interface:

```java
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.datafusion.ScalarUdf;

public final class AddOne implements ScalarUdf {
    @Override
    public FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args) {
        IntVector in = (IntVector) args.get(0);
        IntVector out = new IntVector("add_one", allocator);
        out.allocateNew(in.getValueCount());
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) out.setNull(i);
            else out.set(i, in.get(i) + 1);
        }
        out.setValueCount(in.getValueCount());
        return out;
    }
}
```

Allocate any new vectors — including the result — from the supplied
`BufferAllocator`. The input vectors are read-only views; do not close them.
Ownership of the returned vector transfers to the framework on return.

## Register

```java
try (SessionContext ctx = new SessionContext()) {
    ctx.registerUdf(
        "add_one",
        new AddOne(),
        new ArrowType.Int(32, true),
        List.of(new ArrowType.Int(32, true)),
        Volatility.IMMUTABLE);

    try (DataFrame df = ctx.sql("SELECT add_one(x) FROM t");
         ArrowReader r = df.collect(allocator)) {
        // ...
    }
}
```

The signature is exact: a call must match the declared `argTypes` exactly. Use
`Volatility.IMMUTABLE` for pure functions, `STABLE` for functions that are
deterministic within a single query, and `VOLATILE` for non-deterministic
functions.

## Errors

If the UDF throws, the exception class and message surface in the
`RuntimeException` raised from `collect()`. If the returned vector is `null`,
has the wrong row count, or the wrong type, the runtime raises a
`RuntimeException` with a descriptive message.

## Threading

DataFusion may invoke a UDF concurrently from multiple worker threads. If the
implementation carries mutable state, the implementation must synchronize it.

## Limitations (v1)

- Scalar UDFs only — no aggregates, window functions, or table functions.
- Exact-signature only — no variadic or polymorphic argument lists.
- No nullable-argument short-circuiting; null inputs are passed through to the
  UDF as nulls in the input vector.
