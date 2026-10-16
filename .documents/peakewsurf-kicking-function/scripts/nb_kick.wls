(* Evaluate PeakEWsurf.nb's g*, w(T), Sigma and fm definitions (input cells 1 and 2)
   and export them on a log grid. Argument 1: notebook path; argument 2: output dir. *)
nbPath = $ScriptCommandLine[[2]];
outDir = $ScriptCommandLine[[3]];
nb = Import[nbPath, "NB"];
inputs = Cases[nb, Cell[c_, "Input", ___] :> c, Infinity];
Print["input cells: ", Length[inputs]];
Quiet[ToExpression[inputs[[1]], StandardForm]];
Quiet[ToExpression[inputs[[2]], StandardForm]];
Print["Mpl = ", Mpl, "; table rows = ", Length[NewOmegadata]];

(* the raw w table *)
Export[FileNameJoin[{outDir, "nb_w_table.csv"}], Prepend[N[NewOmegadata], {"T_GeV", "w"}]];

(* Sigma, fmT (closed form) and fmTinterp (what the ODE calls) on a log grid *)
grid = Table[10.^i, {i, -5., Log10[3.*10^4], 1/1000.}];
rows = Quiet@Table[{T, NewOmegaFromData[T], Sigma[T], fmT[T], fmTinterp[T]}, {T, grid}];
Export[FileNameJoin[{outDir, "nb_kick_grid.csv"}],
  Prepend[N[rows], {"T_GeV", "w", "Sigma", "fmT", "fmTinterp"}]];
Print["grid points: ", Length[grid]];

(* symbolic check: pipeline kicking_term (in Mp units) vs notebook rhsPhi's beta term.
   Pipeline: kick = -3 E beta (S + f)/(1 + f), E = G - V/(3H^2), G = 1 - y^2/6,
   with 3 H^2 = (rho (1 + f) + V)/G (Friedmann, Mp = 1). *)
Clear[rho, f, S, V, y, bb];
threeH2 = (rho (1 + f) + V)/(1 - y^2/6);
pipelineKick = -3 ((1 - y^2/6) - V/threeH2) bb (S + f)/(1 + f);
notebookKick = -3 (1 - y^2/6) (bb rho (S + f))/(rho (1 + f) + V);
Print["pipeline - notebook kick (simplified): ", Simplify[pipelineKick - notebookKick]];
