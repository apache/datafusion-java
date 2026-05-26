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

A scalar UDF is a Java-implemented SQL function that operates one row at a time,
expressed in vectorised form: each invocation receives a batch of input columns
and returns either a per-row output column of the same length (`Array`) or a
single value broadcast to every row (`Scalar`).

## Implement

Implement the `ScalarFunction` interface. The implementation declares its own
SQL name, argument fields, return field, and volatility, and supplies the
per-batch `evaluate` body:

```java
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.datafusion.ColumnarValue;
import org.apache.datafusion.ScalarFunction;
import org.apache.datafusion.ScalarFunctionArgs;
import org.apache.datafusion.Volatility;

public final class AddOne implements ScalarFunction {
    private static final ArrowType INT32 = new ArrowType.Int(32, true);

    @Override public String name() { return "add_one"; }
    @Override public List<Field> argFields() { return List.of(Field.nullable("x", INT32)); }
    @Override public Field returnField() { return Field.nullable("y", INT32); }
    @Override public Volatility volatility() { return Volatility.IMMUTABLE; }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
        IntVector in = (IntVector) args.args().get(0).vector();
        IntVector out = new IntVector("add_one", allocator);
        out.allocateNew(in.getValueCount());
        for (int i = 0; i < in.getValueCount(); i++) {
            if (in.isNull(i)) out.setNull(i);
            else out.set(i, in.get(i) + 1);
        }
        out.setValueCount(in.getValueCount());
        return ColumnarValue.array(out);
    }
}
```

Each entry in `args.args()` is a `ColumnarValue` — either `ColumnarValue.Array`
(a per-row vector of length `args.rowCount()`) or `ColumnarValue.Scalar` (a
length-1 vector representing a single literal or folded constant). Access the
underlying Arrow vector with `.vector()`.

Allocate any new vectors — including the result — from the supplied
`BufferAllocator`. The input vectors are read-only views; do not close them.
Ownership of the returned vector transfers to the framework on return.

## Declaring argument and return fields

Each argument and the return value are described as Arrow `Field`s. A `Field`
carries a name, a `FieldType` (Arrow type plus nullability and metadata), and a
list of child fields used by nested types.

For primitive types the `Field.nullable(name, arrowType)` and
`Field.notNullable(name, arrowType)` factories are the shortest form:

```java
Field x = Field.nullable("x", new ArrowType.Int(32, true));
Field y = Field.notNullable("y", new ArrowType.Int(32, true));
```

Nested Arrow types — `List`, `Struct`, `Map`, `Union` — must declare their
element / member / key / value fields as children, because that information
does not live on the `ArrowType` itself. Use the
`new Field(name, FieldType, children)` constructor:

```java
// List<Int32>
ArrowType INT32 = new ArrowType.Int(32, true);
Field listOfInt =
    new Field(
        "vals",
        FieldType.nullable(new ArrowType.List()),
        List.of(Field.nullable("item", INT32)));

// Struct<a: Int32, b: Int32>
Field structAB =
    new Field(
        "ab",
        FieldType.nullable(new ArrowType.Struct()),
        List.of(Field.nullable("a", INT32), Field.nullable("b", INT32)));
```

A UDF declared with `Field.nullable(...)` arguments is registered with an exact
nullability-bearing signature; calls whose argument types do not match exactly
are rejected. The declared `returnField`'s nullability is preserved end-to-end:
a non-nullable return field stays non-nullable in the result schema.

## Returning a Scalar

Functions that yield a single value (nullary constants like `pi()`, or any
function that wants the framework to broadcast a result across the batch) can
return `ColumnarValue.scalar(...)` over a length-1 vector:

```java
public final class JavaPi implements ScalarFunction {
    private static final ArrowType FLOAT64 =
        new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);

    @Override public String name() { return "java_pi"; }
    @Override public List<Field> argFields() { return List.of(); }
    @Override public Field returnField() { return Field.nullable("p", FLOAT64); }
    @Override public Volatility volatility() { return Volatility.VOLATILE; }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
        org.apache.arrow.vector.Float8Vector out =
            new org.apache.arrow.vector.Float8Vector("pi", allocator);
        out.allocateNew(1);
        out.set(0, Math.PI);
        out.setValueCount(1);
        return ColumnarValue.scalar(out);
    }
}
```

The framework expands the scalar across `args.rowCount()` rows automatically.

## Register

Wrap the implementation in a `ScalarUdf` and pass it to
`SessionContext.registerUdf`:

```java
try (SessionContext ctx = new SessionContext()) {
    ctx.registerUdf(new ScalarUdf(new AddOne()));

    try (DataFrame df = ctx.sql("SELECT add_one(x) FROM t");
         ArrowReader r = df.collect(allocator)) {
        // ...
    }
}
```

`ScalarUdf` mirrors DataFusion's `ScalarUDF` struct; `ScalarFunction` mirrors
`ScalarUDFImpl`. Use `Volatility.IMMUTABLE` for pure functions, `STABLE` for
functions that are deterministic within a single query, and `VOLATILE` for
non-deterministic functions.

## Errors

If the UDF throws, the exception class, message, and Java stack trace surface
in the `RuntimeException` raised from `collect()`. The default verbosity is
`FULL` (class + message + stack trace, in `Throwable.printStackTrace` format);
configure it on the session if a less-verbose representation is preferred:

```java
SessionContext.builder()
    .exceptionVerbosity(ExceptionVerbosity.MESSAGE)  // class + message only
    .build();
```

`ExceptionVerbosity.NONE` surfaces only the exception class. The verbosity is
locked at session-construction time and applies to every UDF and table
provider registered against the session.

If the returned `ColumnarValue` is `null`, an Array result's vector length
does not equal `args.rowCount()`, or the result's Arrow type differs from the
declared return field, the runtime raises a `RuntimeException` with a
descriptive message. A Scalar result whose vector is not length-1 is rejected
at the `ColumnarValue.scalar` factory.

## Threading

DataFusion may invoke a UDF concurrently from multiple worker threads. If the
implementation carries mutable state, the implementation must synchronize it.

## Limitations (v1)

- Scalar UDFs only — no aggregates, window functions, or table functions.
- Exact-signature only — no variadic or polymorphic argument lists.
- No nullable-argument short-circuiting; null inputs are passed through to the
  UDF as nulls in the input vector.
