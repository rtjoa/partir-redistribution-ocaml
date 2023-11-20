open Core

type t [@@deriving equal, sexp]

val of_alist : (string * int) list -> t Or_error.t
val axis_size : t -> string -> int Or_error.t
val expect_same_size : t -> string -> string -> unit Or_error.t
val same_size_exn : t -> string -> string -> bool
