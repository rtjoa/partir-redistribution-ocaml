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

  type t = Dim.t Int.Map.t [@@deriving equal, sexp]

  let of_list l = List.mapi l ~f:(fun i dim -> (i, dim)) |> Int.Map.of_alist_exn
  let to_list t = Map.to_alist ~key_order:`Increasing t |> List.map ~f:snd
  let map_list t = to_list t |> List.map
  let global_size : t -> int list = map_list ~f:Dim.global_size
  let local_size : t -> int list = map_list ~f:Dim.local_size
end

module Collective = struct
  type t =
    | All_gather of int
    | Dyn_slice of int * string
    | All_to_all of int * int
    (* All_permute components *)
    | Swap_eq_size_tops of int * int
    | Swap_for_eq_size_replicated of int * string * string
    | Swap_within of int * string * string
  [@@deriving equal, sexp]
end

let fully_undistribute (ty : Array_type.t) =
  Array_type.to_list ty
  |> List.concat_mapi ~f:(fun i dim ->
         Array_type.Dim.axes dim
         |> List.map ~f:(fun _axis -> Collective.All_gather i))

let fully_distribute_to (ty : Array_type.t) =
  Array_type.to_list ty
  |> List.concat_mapi ~f:(fun i dim ->
         Array_type.Dim.axes dim
         |> List.rev_map ~f:(fun axis -> Collective.Dyn_slice (i, axis)))

let update_existing_exn map key ~f =
  Map.update map key ~f:(fun data_opt -> f (Option.value_exn data_opt))

let rec perform_one (mesh : Mesh.t) (collective : Collective.t)
    (ty : Array_type.t) : Array_type.t =
  let open Array_type.Dim in
  match collective with
  | All_gather i ->
      update_existing_exn ty i ~f:(fun { local_size; axes; global_size } ->
          let axis_size = Mesh.axis_size_exn mesh (List.hd_exn axes) in
          {
            local_size = local_size * axis_size;
            axes = List.tl_exn axes;
            global_size;
          })
  | Dyn_slice (i, axis) ->
      update_existing_exn ty i ~f:(fun { local_size; axes; global_size } ->
          let axis_size = Mesh.axis_size_exn mesh axis in
          assert (local_size % axis_size = 0);
          {
            local_size = local_size /% axis_size;
            axes = axis :: axes;
            global_size;
          })
  | All_to_all (from_dim, to_dim) ->
      let axis =
        Map.find_exn ty from_dim |> Array_type.Dim.axes |> List.hd_exn
      in
      perform_one mesh (Collective.All_gather from_dim) ty
      |> perform_one mesh (Collective.Dyn_slice (to_dim, axis))
  | Swap_eq_size_tops (i, j) ->
      let axis_i = Map.find_exn ty i |> Array_type.Dim.axes |> List.hd_exn in
      let axis_j = Map.find_exn ty j |> Array_type.Dim.axes |> List.hd_exn in
      assert (Mesh.axis_size_exn mesh axis_i = Mesh.axis_size_exn mesh axis_j);
      let ty' =
        perform_one mesh (Collective.All_gather i) ty
        |> perform_one mesh (Collective.All_gather j)
        |> perform_one mesh (Collective.Dyn_slice (i, axis_j))
        |> perform_one mesh (Collective.Dyn_slice (j, axis_i))
      in
      assert (
        List.equal Int.equal
          (Array_type.global_size ty)
          (Array_type.global_size ty'));
      assert (
        List.equal Int.equal (Array_type.local_size ty)
          (Array_type.local_size ty'));
      ty'
  | Swap_for_eq_size_replicated (i, axis_i, replicated_axis) ->
      raise_s [%message "Unimplemented!"]
  | _ -> raise_s [%message "Unimplemented!"]

(* todo: assert local and global sizes stay same for all allpermute*)

let perform (mesh : Mesh.t) (pgrm : Collective.t list) (ty : Array_type.t) =
  List.fold pgrm ~init:ty ~f:(fun ty collective ->
      perform_one mesh collective ty)

(* "Easy redistribution" as described at the top of Sec. 4.3 *)
let redistribute_easy (src : Array_type.t) (target : Array_type.t) =
  List.append (fully_undistribute src) (fully_distribute_to target)

let redistribute (mesh : Mesh.t) (src : Array_type.t) (target : Array_type.t) :
    Collective.t list =
  let pgrm = redistribute_easy src target in
  pgrm

let step_to_normal_form (mesh : Mesh.t) (src : Array_type.t)
    (pgrm : Collective.t list) : Collective.t list =
  raise_s [%message "TODO"]

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
  Array_type.map_list ~f:(fun dim -> base_offset_dim mesh dim i) ty

let%expect_test "base offset" =
  let mesh : Mesh.t =
    Mesh.create_exn [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ]
  in
  let ty =
    Array_type.of_list
      [
        Array_type.Dim.create 3 [ "x1"; "x2" ] 12;
        Array_type.Dim.create 2 [ "y1"; "y2" ] 12;
      ]
  in
  let res =
    base_offset mesh ty
      (String.Map.of_alist_exn [ ("x1", 2); ("x2", 10); ("y1", 0); ("y2", 1) ])
  in
  print_s [%sexp (res : int list)];
  [%expect {| (66 6) |}]

let%expect_test "redistribute" =
  let mesh : Mesh.t =
    Mesh.create_exn [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ]
  in
  let src =
    Array_type.of_list
      [
        Array_type.Dim.create 3 [ "x1"; "x2" ] 12;
        Array_type.Dim.create 2 [ "y1"; "y2" ] 12;
      ]
  in
  let target =
    Array_type.of_list
      [
        Array_type.Dim.create 2 [ "y1"; "y2" ] 12;
        Array_type.Dim.create 3 [ "x1"; "x2" ] 12;
      ]
  in
  let pgrm = redistribute mesh src target in
  print_s [%sexp (pgrm : Collective.t list)];
  [%expect
    {|
    ((All_gather 0) (All_gather 0) (All_gather 1) (All_gather 1) (Dyn_slice 0 y2)
     (Dyn_slice 0 y1) (Dyn_slice 1 x2) (Dyn_slice 1 x1)) |}]
