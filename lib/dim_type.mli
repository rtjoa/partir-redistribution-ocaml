open Core

type t [@@deriving compare, equal, sexp]

val create : local_size:int -> axes:string list -> global_size:int -> t

(* Basic properties *)
val local_size : t -> int
val axes : t -> string list
val global_size : t -> int
val top_axis : t -> string Or_error.t
val has_axis : t -> axis:string -> bool
val invariant : Mesh.t -> t -> unit Or_error.t

(* Operations *)
val gather : Mesh.t -> t -> t Or_error.t
val slice : Mesh.t -> string -> t -> t Or_error.t
val swap_within : string -> string -> t -> t Or_error.t
