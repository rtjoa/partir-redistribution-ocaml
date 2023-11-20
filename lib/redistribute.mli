open Core

module Collective : sig
  type t =
    | All_gather of int
    | Dyn_slice of int * string
    | All_to_all of int * int
    | Swap_eq_size_tops of int * int
    | Swap_top_for_eq_size_replicated of int * string
    | Swap_within of int * string * string
end

(* Find the result type given an initial type and a series of collectives
   performed on an array of that type *)
val interpret :
  Mesh.t -> Collective.t list -> Array_type.t -> Array_type.t Or_error.t

(* Transform a sequence of collectives acting on some source array to normal
   form *)
val to_normal_form :
  Mesh.t -> Array_type.t -> Collective.t list -> Collective.t list Or_error.t

(* "Easy redistribution" as described at the top of Sec. 4.3. Generates a
   simple, memory-inefficient sequence of collectives to redistribute from one
   array type to another *)
val redistribute_easy : Array_type.t -> Array_type.t -> Collective.t list

(* Generate a sequence of collectives in normal form to redistribute from one
   array type to another *)
val redistribute :
  Mesh.t -> Array_type.t -> Array_type.t -> Collective.t list Or_error.t
