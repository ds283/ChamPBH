(* Run PeakEWsurf.nb's NDSolve for each (beta, Sigma variant) pair; see swap_defs.wl for the arguments. *)
Get[FileNameJoin[{DirectoryName[$ScriptCommandLine[[1]]], "swap_defs.wl"}]];

jobs = Tuples[{betas, variants}];
res = If[par,
  LaunchKernels[Min[Length[jobs], $ProcessorCount - 2]];
  DistributeDefinitions[run, sigmaOf, SigmaNB, SigmaPipe, SigmaEW, SigmaPipeInterp, lnTlo, lnThi, TSWITCH,
    rhsPhi, PhiPrimeCoeff1, RhoRadA, A, Vexp, VexpPrime, NewOmegaFromData, NewOmegaFromDataInterp,
    fmTinterp, Gstarrhointerp, GstarsDerivInterp, Gstarsinterp, Mpl, v0, M, phiInit, maxStep, outDir, tag];
  ParallelMap[run, jobs, Method -> "FinestGrained"],
  Map[run, jobs]];

Do[Print[StringRiffle[ToString /@ {r[[1]], r[[2]], NumberForm[r[[3]], 4], NumberForm[r[[4]], 8], NumberForm[r[[5]], 6]}, " | "],
    "\n    turning points (N, T_J GeV, phi, type): ", N[r[[6]], 7]], {r, res}];
Export[FileNameJoin[{outDir, tag <> "_summary.m"}], res];
