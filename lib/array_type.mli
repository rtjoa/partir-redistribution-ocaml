open Core

type t [@@deriving compare, equal, sexp]

val of_list : Dim_type.t list -> t
val to_list : t -> Dim_type.t list

(* Properties *)
val get_dim : t -> int -> Dim_type.t Or_error.t
val has_axis : t -> string -> bool
val top_axis : t -> int -> string Or_error.t
val local_type : t -> int list
val global_type : t -> int list
val local_size : t -> int
val invariant : Mesh.t -> t -> unit Or_error.t

(* Operations *)
val gather : Mesh.t -> t -> int -> t Or_error.t
val slice : Mesh.t -> t -> int -> string -> t Or_error.t

val update_dim :
  t -> int -> f:(Dim_type.t -> Dim_type.t Or_error.t) -> t Or_error.t

(* Calculate base offset map *)
val base_offset : Mesh.t -> t -> int String.Map.t -> int list Or_error.t
