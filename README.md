# partir-redistribution-ocaml

This repository implements constructs from *[Memory-efficient array redistribution
through portable collective communication](https://arxiv.org/abs/2112.01075)*,
mainly to solve the **memory-constrained redistribution problem** (Section 4.3).

The most notable functions are listed in
[`lib/redistribute.mli`](lib/redistribute.mli) and summarized here:

```ocaml
(* Transform an array type via a sequence of collectives *)
val interpret :
  Mesh.t -> Collective.t list -> Array_type.t -> Array_type.t Or_error.t

(* Transform a sequence of collectives to normal form *)
val to_normal_form :
  Mesh.t -> Array_type.t -> Collective.t list -> Collective.t list Or_error.t

(* Generate a redistribution program between array types in normal form *)
val redistribute :
  Mesh.t -> Array_type.t -> Array_type.t -> Collective.t list Or_error.t
```

## Installation

After installing `dune`, `core`, and `ppx_jane` via opam, run tests with `dune runtest`.

## Visual Example
Given mesh of `{"x": 2, "y": 3}`, we can redistribute from `[3{"x"}6]` to
`[2{"y"}6]` with the following collectives.

Let the full contents of the undistributed array be `[0, ..., 5]`.

```
[3{"x"}6]
x↓ y→  0    1    2
 0   012  012  012
 1   345  345  345

-- dynslice(0, "y") -->

[1{"y", "x"}6]
x↓ y→  0    1    2
 0     0    1    2
 1     3    4    5

-- allpermute -->

[1{"x", "y"}6]
x↓ y→  0    1    2
 0     0    2    4
 1     1    3    5

-- allgather(0) -->

[2{"y"}6]
x↓ y→  0    1    2
 0    01   23   45
 1    01   23   45
```
