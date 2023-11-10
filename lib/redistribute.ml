open Core

module Axis = struct
  type t = { name : string; size : int } [@@deriving equal, fields, sexp]

  let create_exn name size =
    if not (Util.is_prime size) then
      raise_s [%message "Axes must have prime size" (size : int)]
    else { name; size }
end

module Mesh = struct
  type t = Axis.t list [@@deriving sexp, equal]

  let has_axis t = List.mem t ~equal:Axis.equal
end

module Array_type = struct
  module Dim = struct
    type t = { tile_size : int; axes : Axis.t list; global_size : int }
    [@@deriving equal, sexp]

    let create_exn mesh tile_size axes global_size =
      let expected_global_size =
        List.fold axes ~init:1 ~f:(fun prod axis -> prod * Axis.size axis)
        * tile_size
      in
      if not (Int.equal expected_global_size global_size) then
        raise_s [%message (expected_global_size : int) (global_size : int)];
      List.iter axes ~f:(fun axis ->
          if not (Mesh.has_axis mesh axis) then
            raise_s
              [%message "Axis not in mesh" (axis : Axis.t) (mesh : Mesh.t)]);
      { tile_size; axes; global_size }
  end

  type t = Dim.t list * Mesh.t [@@deriving equal, sexp]

  let create_exn (mesh : Mesh.t) (dim_tuples : (int * Axis.t list * int) list) =
    ( List.map
        ~f:(fun (tile_size, axes, global_size) ->
          Dim.create_exn mesh tile_size axes global_size)
        dim_tuples,
      mesh )
end

module Collective = struct
  type t =
    | All_gather of int
    | Dyn_slice of int * string
    | All_to_all of int * int
    | All_permute of Array_type.t
  [@@deriving equal, sexp]
end

let redistribute (src : Array_type.t) (target : Array_type.t) :
    Collective.t list =
  let () = ignore (src, target) in
  []

let%expect_test "redistribute" =
  let axis_x1 = Axis.create_exn "x1" 2 in
  let axis_x2 = Axis.create_exn "x2" 2 in
  let axis_y1 = Axis.create_exn "y1" 3 in
  let axis_y2 = Axis.create_exn "y2" 2 in
  let mesh : Mesh.t = [ axis_x1; axis_x2; axis_y1; axis_y2 ] in
  let src =
    Array_type.create_exn mesh
      [ (3, [ axis_x1; axis_x2 ], 12); (2, [ axis_y1; axis_y2 ], 12) ]
  in
  let target =
    Array_type.create_exn mesh
      [ (2, [ axis_y1; axis_y2 ], 12); (3, [ axis_x1; axis_x2 ], 12) ]
  in
  let pgrm = redistribute src target in
  print_s [%sexp (pgrm : Collective.t list)];
  [%expect {| () |}]
