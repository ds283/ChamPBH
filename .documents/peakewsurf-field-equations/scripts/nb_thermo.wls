(* Export PeakEWsurf.nb's thermodynamic functions (input cells 1-2) on a log grid.
   Args: notebook path, output dir. *)
nb = Import[$ScriptCommandLine[[2]], "NB"]; outDir = $ScriptCommandLine[[3]];
inputs = Cases[nb, Cell[c_, "Input", ___] :> c, Infinity];
Quiet[ToExpression[inputs[[1]], StandardForm]];
Quiet[ToExpression[inputs[[2]], StandardForm]];
grid = Table[10.^i, {i, -5., Log10[2.*10^4] - 10^-6, 1/2000.}];
rows = Quiet@Table[{T, Gstarrhointerp[T], Gstarsinterp[T], T*GstarsDerivInterp[T], T*GstarrhoDerivInterp[T],
     fullgstarrho[T], fullgstars[T], Sigma[T], fmTinterp[T]}, {T, grid}];
Export[FileNameJoin[{outDir, "nb_thermo_grid.csv"}],
  Prepend[N[rows], {"T_GeV", "grho_interp", "gs_interp", "dgs_dlnT", "dgrho_dlnT", "grho_raw", "gs_raw", "Sigma", "fm"}]];
Print["rows: ", Length[rows], "  Mpl=", Mpl];
