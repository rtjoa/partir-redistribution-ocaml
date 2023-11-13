open Core

module Mesh = struct
  type t = { axes : int String.Map.t; axes_order : string list }
  [@@deriving equal, fields, sexp]

  let create_exn alist =
    {
      axes = String.Map.of_alist_exn alist;
      axes_order = List.map ~f:(fun (name, _size) -> name) alist;
    }

  let axis_size_exn t (axis_name : string) : int = Map.find_exn t.axes axis_name
end

module Array_type = struct
  module Dim = struct
    type t = { local_size : int; axes : string list; global_size : int }
    [@@deriving equal, fields, sexp]

    let create local_size axes global_size = { local_size; axes; global_size }
  end

  type t = Dim.t list [@@deriving equal, sexp]
end

module Collective = struct
  type t =
    | All_gather of int
    | Dyn_slice of int * string
    | All_to_all of int * int
    | All_permute of Array_type.t
  [@@deriving equal, sexp]
end

let redistribute (mesh : Mesh.t) (src : Array_type.t) (target : Array_type.t) :
    Collective.t list =
  let () = ignore (src, target) in
  []

let rec base_offset_dim mesh (dim : Array_type.Dim.t) (i : int String.Map.t) :
    int =
  match dim with
  | { local_size; axes = []; global_size } ->
      assert (local_size = global_size);
      0
  | { local_size; axes = axis :: rest; global_size } ->
      (local_size * Map.find_exn i axis)
      + base_offset_dim mesh
          (Array_type.Dim.create
             (local_size * Mesh.axis_size_exn mesh axis)
             rest global_size)
          i

let base_offset mesh (ty : Array_type.t) (i : int String.Map.t) : int list =
  List.map ~f:(fun dim -> base_offset_dim mesh dim i) ty

let%expect_test "base offset" =
  let mesh : Mesh.t =
    Mesh.create_exn [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ]
  in
  let ty =
    [
      Array_type.Dim.create 3 [ "x1"; "x2" ] 12;
      Array_type.Dim.create 2 [ "y1"; "y2" ] 12;
    ]
  in
  let res =
    base_offset mesh ty
      (String.Map.of_alist_exn [ ("x1", 1); ("x2", 10); ("y1", 0); ("y2", 1) ])
  in
  print_s [%sexp (res : int list)];
  [%expect {| (63 6) |}]

let%expect_test "redistribute" =
  let mesh : Mesh.t =
    Mesh.create_exn [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ]
  in
  let src =
    [
      Array_type.Dim.create 3 [ "x1"; "x2" ] 12;
      Array_type.Dim.create 2 [ "y1"; "y2" ] 12;
    ]
  in
  let target =
    [
      Array_type.Dim.create 2 [ "y1"; "y2" ] 12;
      Array_type.Dim.create 3 [ "x1"; "x2" ] 12;
    ]
  in
  let pgrm = redistribute mesh src target in
  print_s [%sexp (pgrm : Collective.t list)];
  [%expect {| () |}]
