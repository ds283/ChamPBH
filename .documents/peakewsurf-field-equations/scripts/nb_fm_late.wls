(* Export the notebook's matter fraction (the closed form fmT and the interpolant fmTinterp that the
   ODE calls) and its w, from 2.35e-13 GeV (T_CMB) to 2e4 GeV, 200 points per decade.
   Args: notebook path, output dir. *)
nb = Import[$ScriptCommandLine[[2]], "NB"]; outDir = $ScriptCommandLine[[3]];
inputs = Cases[nb, Cell[c_, "Input", ___] :> c, Infinity];
Quiet[ToExpression[inputs[[#]], StandardForm]] & /@ {1, 2};
grid = Table[10.^i, {i, Log10[2.36*10^-13], Log10[2.*10^4] - 10^-6, 1/200.}];
rows = Quiet@Table[{T, NewOmegaFromData[T], fmT[T], fmTinterp[T]}, {T, grid}];
Export[FileNameJoin[{outDir, "nb_fm_late.csv"}], Prepend[N[rows], {"T_GeV", "w", "fmT", "fmTinterp"}]];
Print[Length[rows], " rows"];
