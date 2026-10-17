(* Export the notebook's g_s interpolation nodes (Gstarsdata, 1e-5..2.1e4 GeV) and its Sigma on a 20000/decade grid.
   Args: notebook path, out dir. *)
nb = Import[$ScriptCommandLine[[2]], "NB"]; outDir = $ScriptCommandLine[[3]];
inputs = Cases[nb, Cell[c_, "Input", ___] :> c, Infinity];
Quiet[ToExpression[inputs[[#]], StandardForm]] & /@ {1, 2};
nodes = Select[N[Gstarsdata], 0.9*10^-5 <= #[[1]] <= 2.1*10^4 &];
Export[FileNameJoin[{outDir, "nb_gs_nodes.csv"}], Prepend[nodes, {"T_GeV", "gs"}]];
grid = Table[10.^i, {i, -5., Log10[400.], 1/20000.}];
Export[FileNameJoin[{outDir, "nb_sigma_fine.csv"}], Prepend[Quiet@Table[{T, Sigma[T]}, {T, grid}], {"T_GeV", "Sigma"}]];
Print[Length[nodes], " nodes; ", Length[grid], " Sigma points"];
