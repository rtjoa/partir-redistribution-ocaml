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
    [@@deriving compare, equal, fields, sexp]

    let create local_size axes global_size = { local_size; axes; global_size }
  end

  type t = Dim.t Int.Map.t [@@deriving compare, equal, sexp]

  let of_list l = List.mapi l ~f:(fun i dim -> (i, dim)) |> Int.Map.of_alist_exn
  let to_list t = Map.to_alist ~key_order:`Increasing t |> List.map ~f:snd
  let map_list t = to_list t |> List.map
  let global_type : t -> int list = map_list ~f:Dim.global_size
  let local_type : t -> int list = map_list ~f:Dim.local_size
  let local_size t : int = List.fold (local_type t) ~init:1 ~f:Int.( * )
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

let get_top_axis_exn (ty : Array_type.t) (i : int) =
  Map.find_exn ty i |> Array_type.Dim.axes |> List.hd_exn

let get_top_two_axes_exn (ty : Array_type.t) (i : int) =
  match Map.find_exn ty i |> Array_type.Dim.axes with
  | x :: y :: _ -> (x, y)
  | _ -> raise_s [%message "Expected >=2 axes" (ty : Array_type.t) (i : int)]

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
      Map.iter ty ~f:(fun dim ->
          assert (
            not (List.mem (Array_type.Dim.axes dim) axis ~equal:String.equal)));
      update_existing_exn ty i ~f:(fun { local_size; axes; global_size } ->
          let axis_size = Mesh.axis_size_exn mesh axis in
          assert (local_size % axis_size = 0);
          (* Axis cannot appear in multiple dimensions *)
          {
            local_size = local_size /% axis_size;
            axes = axis :: axes;
            global_size;
          })
  | All_to_all (from_dim, to_dim) ->
      let axis = get_top_axis_exn ty from_dim in
      perform_one mesh (Collective.All_gather from_dim) ty
      |> perform_one mesh (Collective.Dyn_slice (to_dim, axis))
  | Swap_eq_size_tops (i, j) ->
      let axis_i = get_top_axis_exn ty i in
      let axis_j = get_top_axis_exn ty j in
      assert (Mesh.axis_size_exn mesh axis_i = Mesh.axis_size_exn mesh axis_j);
      let ty' =
        perform_one mesh (Collective.All_gather i) ty
        |> perform_one mesh (Collective.All_gather j)
        |> perform_one mesh (Collective.Dyn_slice (i, axis_j))
        |> perform_one mesh (Collective.Dyn_slice (j, axis_i))
      in
      (* Assert local and global sizes stay same for all allpermute*)
      assert (
        List.equal Int.equal
          (Array_type.global_type ty)
          (Array_type.global_type ty'));
      assert (
        List.equal Int.equal (Array_type.local_type ty)
          (Array_type.local_type ty'));
      ty'
  | Swap_for_eq_size_replicated (i, axis_i, replicated_axis) ->
      (* Axis cannot appear in multiple dimensions *)
      Map.iter ty ~f:(fun dim ->
          assert (
            not
              (List.mem (Array_type.Dim.axes dim) replicated_axis
                 ~equal:String.equal)));
      update_existing_exn ty i ~f:(fun { local_size; axes; global_size } ->
          let axis_i_idx =
            List.findi_exn axes ~f:(fun _idx axis -> String.equal axis axis_i)
            |> fst
          in
          {
            local_size;
            axes =
              List.mapi axes ~f:(fun idx axis ->
                  if idx = axis_i_idx then replicated_axis else axis);
            global_size;
          })
  | Swap_within (i, axis_i, axis_j) ->
      update_existing_exn ty i ~f:(fun { local_size; axes; global_size } ->
          {
            local_size;
            axes =
              List.map axes ~f:(fun axis ->
                  if String.equal axis axis_i then axis_j
                  else if String.equal axis axis_j then axis_i
                  else axis);
            global_size;
          })

let perform (mesh : Mesh.t) (pgrm : Collective.t list) (ty : Array_type.t) =
  List.fold pgrm ~init:ty ~f:(fun ty collective ->
      perform_one mesh collective ty)

let perform_with_history (mesh : Mesh.t) (pgrm : Collective.t list)
    (ty : Array_type.t) =
  List.fold pgrm ~init:[ ty ] ~f:(fun tys collective ->
      perform_one mesh collective (List.hd_exn tys) :: tys)

module Collective_with_explicit_axes = struct
  type t =
    | All_gather of int * string
    | Dyn_slice of int * string
    | All_to_all of int * int * string
    | Swap_eq_size_tops of int * int * string * string
    | Swap_for_eq_size_replicated of int * string * string
    | Swap_within of int * string * string

  let create (collective : Collective.t) src =
    match collective with
    | All_gather i -> All_gather (i, get_top_axis_exn src i)
    | Dyn_slice (i, x) -> Dyn_slice (i, x)
    | All_to_all (i, j) -> All_to_all (i, j, get_top_axis_exn src i)
    | Swap_eq_size_tops (i, j) ->
        Swap_eq_size_tops (i, j, get_top_axis_exn src i, get_top_axis_exn src j)
    | Swap_for_eq_size_replicated (i, x, y) ->
        Swap_for_eq_size_replicated (i, x, y)
    | Swap_within (i, x, y) -> Swap_within (i, x, y)
end

(* This function is more-or-less the constructive proof Lemmas 4.6 and 4.7 *)
let fix_adjacent_collectives mesh src c1 c2 =
  let open Collective in
  match
    ( Collective_with_explicit_axes.create c1 src,
      Collective_with_explicit_axes.create c2 (perform_one mesh c1 src) )
  with
  (* Flatten peak: /\ ~~> \epsilon or \_/ or -- *)
  | All_gather (i, ax_i), Dyn_slice (j, ax_j) ->
      if i = j && String.equal ax_i ax_j then []
      else if i = j && not (String.equal ax_i ax_j) then
        if Mesh.axis_size_exn mesh ax_i = Mesh.axis_size_exn mesh ax_j then
          [ Swap_for_eq_size_replicated (i, ax_i, ax_j) ]
        else [ Dyn_slice (i, ax_j); Swap_within (i, ax_j, ax_i); All_gather i ]
      else if i <> j && String.equal ax_i ax_j then [ All_to_all (i, j) ]
      else [ Dyn_slice (j, ax_j); All_gather i ]
  (* Move rising edge later /¯ ~~> _/ *)
  | All_gather (i, ax_i), All_to_all (k, l, ax_k) ->
      if i = k then
        [ Swap_within (i, ax_i, ax_k); All_to_all (i, l); All_gather i ]
      else if i = l then
        if Mesh.axis_size_exn mesh ax_i = Mesh.axis_size_exn mesh ax_k then
          [ Swap_eq_size_tops (i, k); All_gather k ]
        else [ All_to_all (k, i); Swap_within (i, ax_i, ax_k); All_gather i ]
      else [ All_to_all (k, l); All_gather i ]
  | All_gather (i, ax_i), Swap_eq_size_tops (k, l, ax_k, ax_l) ->
      let k, l, ax_k, ax_z =
        if i = l then (l, k, ax_k, ax_l) else (k, l, ax_k, ax_l)
      in
      if i = k then
        [
          Swap_within (i, ax_i, ax_k);
          Swap_eq_size_tops (i, l);
          Swap_within (i, ax_i, ax_l);
          All_gather i;
        ]
      else [ Swap_eq_size_tops (k, l); All_gather i ]
  | All_gather (i, _), Swap_within (k, ax_k1, ax_k2) ->
      (* These commute even if i=k; all mentioned axes must be distinct *)
      [ c2; c1 ]
  | All_gather (i, _), Swap_for_eq_size_replicated (k, ax_k, ax_replicated) ->
      (* These commute even if i=k; all mentioned axes must be distinct *)
      [ c2; c1 ]
  (* Move falling edge earlier: ¯\ ~~> \_ *)
  | All_to_all (k, l, ax_k), Dyn_slice (i, ax_i) ->
      if i = k then
        [ Dyn_slice (i, ax_i); Swap_within (i, ax_i, ax_k); All_to_all (i, l) ]
      else if i = l then
        [ Dyn_slice (i, ax_i); All_to_all (k, i); Swap_within (i, ax_i, ax_k) ]
      else [ Dyn_slice (i, ax_i); All_to_all (k, l) ]
  | Swap_eq_size_tops (k, l, ax_k, ax_l), Dyn_slice (i, ax_i) ->
      let k, l, ax_k, ax_z =
        if i = l then (l, k, ax_k, ax_l) else (k, l, ax_k, ax_l)
      in
      if i = k then [ Dyn_slice (i, ax_i); Swap_within (i, ax_i, ax_k) ]
      else [ Dyn_slice (i, ax_i); Swap_eq_size_tops (k, l) ]
  | Swap_within (k, ax_k1, ax_k2), Dyn_slice (i, ax_i) -> [ c2; c1 ]
  | Swap_for_eq_size_replicated (k, ax_k, ax_replicated), Dyn_slice (i, ax_i) ->
      [ c2; c1 ]
  (* Flat-flat *)
  | ( ( All_to_all _ | Swap_eq_size_tops _ | Swap_for_eq_size_replicated _
      | Swap_within _ ),
      ( All_to_all _ | Swap_eq_size_tops _ | Swap_for_eq_size_replicated _
      | Swap_within _ ) )
  (* Flat-rising *)
  | ( ( All_to_all _ | Swap_eq_size_tops _ | Swap_for_eq_size_replicated _
      | Swap_within _ ),
      All_gather _ )
  (* Falling-flat *)
  | ( Dyn_slice _,
      ( All_to_all _ | Swap_eq_size_tops _ | Swap_for_eq_size_replicated _
      | Swap_within _ ) )
  (* Falling-rising *)
  | Dyn_slice _, All_gather _
  (* Rising-rising *)
  | All_gather _, All_gather _
  (* Falling-falling *)
  | Dyn_slice _, Dyn_slice _ ->
      [ c1; c2 ]

let fix_adjacent_collectives_and_check mesh src c1 c2 =
  let cs = fix_adjacent_collectives mesh src c1 c2 in
  assert (Array_type.equal (perform mesh cs src) (perform mesh [ c1; c2 ] src));
  cs

let rec step_to_normal_form (mesh : Mesh.t) (src : Array_type.t)
    (pgrm : Collective.t list) : Collective.t list =
  match pgrm with
  | c1 :: c2 :: rest -> (
      match fix_adjacent_collectives_and_check mesh src c1 c2 with
      | c :: cs ->
          c :: step_to_normal_form mesh (perform_one mesh c src) (cs @ rest)
      | [] -> step_to_normal_form mesh src rest)
  | [ _ ] | [] -> pgrm

let rec to_normal_form (mesh : Mesh.t) (src : Array_type.t)
    (pgrm : Collective.t list) : Collective.t list =
  let pgrm' = step_to_normal_form mesh src pgrm in
  assert (Array_type.equal (perform mesh pgrm src) (perform mesh pgrm' src));
  if List.equal Collective.equal pgrm' pgrm then pgrm'
  else to_normal_form mesh src pgrm'

(* "Easy redistribution" as described at the top of Sec. 4.3 *)
let redistribute_easy (src : Array_type.t) (target : Array_type.t) =
  List.append (fully_undistribute src) (fully_distribute_to target)

let redistribute (mesh : Mesh.t) (src : Array_type.t) (target : Array_type.t) :
    Collective.t list =
  to_normal_form mesh src (redistribute_easy src target)

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

let%expect_test "redistribute easy" =
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
  let pgrm = redistribute_easy src target in
  print_s [%sexp (pgrm : Collective.t list)];
  [%expect
    {|
    ((All_gather 0) (All_gather 0) (All_gather 1) (All_gather 1) (Dyn_slice 0 y2)
     (Dyn_slice 0 y1) (Dyn_slice 1 x2) (Dyn_slice 1 x1)) |}];
  let res = perform mesh pgrm src in
  [%test_eq: Array_type.t] res target;
  print_s [%sexp (res : Array_type.t)];
  [%expect
    {|
    ((0 ((local_size 2) (axes (y1 y2)) (global_size 12)))
     (1 ((local_size 3) (axes (x1 x2)) (global_size 12)))) |}];
  let path = perform_with_history mesh pgrm src in
  print_s [%sexp (path : Array_type.t list)];
  [%expect
    {|
    (((0 ((local_size 2) (axes (y1 y2)) (global_size 12)))
      (1 ((local_size 3) (axes (x1 x2)) (global_size 12))))
     ((0 ((local_size 2) (axes (y1 y2)) (global_size 12)))
      (1 ((local_size 6) (axes (x2)) (global_size 12))))
     ((0 ((local_size 2) (axes (y1 y2)) (global_size 12)))
      (1 ((local_size 12) (axes ()) (global_size 12))))
     ((0 ((local_size 6) (axes (y2)) (global_size 12)))
      (1 ((local_size 12) (axes ()) (global_size 12))))
     ((0 ((local_size 12) (axes ()) (global_size 12)))
      (1 ((local_size 12) (axes ()) (global_size 12))))
     ((0 ((local_size 12) (axes ()) (global_size 12)))
      (1 ((local_size 6) (axes (y2)) (global_size 12))))
     ((0 ((local_size 12) (axes ()) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))
     ((0 ((local_size 6) (axes (x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))) |}];
  let local_sizes = List.map path ~f:Array_type.local_size in
  print_s [%sexp (local_sizes : int list)];
  [%expect {| (6 12 24 72 144 72 24 12 6) |}]

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
    ((Swap_within 1 y1 y2) (Swap_within 0 x1 x2) (Swap_eq_size_tops 0 1)
     (Swap_within 0 x1 y2) (Swap_within 1 x2 y1) (All_to_all 1 0)
     (Swap_within 0 x1 y1) (All_to_all 0 1)) |}];
  let path = perform_with_history mesh pgrm src in
  print_s [%sexp (path : Array_type.t list)];
  [%expect{|
    (((0 ((local_size 2) (axes (y1 y2)) (global_size 12)))
      (1 ((local_size 3) (axes (x1 x2)) (global_size 12))))
     ((0 ((local_size 1) (axes (x1 y1 y2)) (global_size 12)))
      (1 ((local_size 6) (axes (x2)) (global_size 12))))
     ((0 ((local_size 1) (axes (y1 x1 y2)) (global_size 12)))
      (1 ((local_size 6) (axes (x2)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 y2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 x2)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 y2)) (global_size 12)))
      (1 ((local_size 2) (axes (x2 y1)) (global_size 12))))
     ((0 ((local_size 3) (axes (y2 x1)) (global_size 12)))
      (1 ((local_size 2) (axes (x2 y1)) (global_size 12))))
     ((0 ((local_size 3) (axes (x2 x1)) (global_size 12)))
      (1 ((local_size 2) (axes (y2 y1)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y2 y1)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))) |}];
  let local_sizes = List.map path ~f:Array_type.local_size in
  print_s [%sexp (local_sizes : int list)];
  [%expect{| (6 6 6 6 6 6 6 6 6) |}]
