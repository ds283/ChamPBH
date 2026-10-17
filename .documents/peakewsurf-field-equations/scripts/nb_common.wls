(* Run PeakEWsurf.nb's NDSolve with the thermodynamic inputs swapped for the pipeline's.
   Args: notebook path, out dir, pipeline thermo grid CSV (thermo_compare.py), beta list,
         variants, MaxStepSize, phi_i, N_max, tag, parallel.
   Variants
     nb      : the notebook unchanged (its own Sigma, gamma = |T g_s'/g_s|/3, f_m)
     pipeSig : the pipeline's Sigma; the notebook's gamma and f_m      (= "pipe" of the first audit)
     common  : the pipeline's Sigma, gamma = (d g_s/d ln T)/(3 g_s) and f_m(T)
     commonV0: as common, with V and V' set to zero                    (tests that V is negligible)
   Everything else (rhsPhi, PhiPrimeCoeff1, rhsPhiLog, coeffLog, lnRho, Vr, RhoRadA, initial data, NDSolve options) is the notebook's. *)
args = $ScriptCommandLine;
{nbPath, outDir, thermoCSV} = args[[2 ;; 4]];
betas = ToExpression[args[[5]]]; variants = ToExpression[args[[6]]];
maxStep = ToExpression[args[[7]]]; phiInit = ToExpression[args[[8]]]; Nmax = ToExpression[args[[9]]];
tag = args[[10]]; par = ToExpression[args[[11]]];
(* optional: decimals in the output file names (default 5), and a per-run time limit in seconds *)
digits = If[Length[args] >= 12, ToExpression[args[[12]]], 5];
tlimit = If[Length[args] >= 13, ToExpression[args[[13]]], Infinity];

nb = Import[nbPath, "NB"];
inputs = Cases[nb, Cell[c_, "Input", ___] :> c, Infinity];
Quiet[ToExpression[inputs[[1]], StandardForm]];
Quiet[ToExpression[inputs[[2]], StandardForm]];
Quiet[ToExpression[inputs[[3]], StandardForm]];

th = Rest@Import[thermoCSV, "CSV"];   (* T_GeV, Sigma_pipe, gs_pipe, dgs_dlnT_pipe, fm_pipe *)
lnTs = Log[th[[All, 1]]];
SigPipeI = Interpolation[Transpose[{lnTs, th[[All, 2]]}], InterpolationOrder -> 3];
gamPipeI = Interpolation[Transpose[{lnTs, th[[All, 4]]/(3 th[[All, 3]])}], InterpolationOrder -> 3];
lnfPipeI = Interpolation[Transpose[{lnTs, Log[th[[All, 5]]]}], InterpolationOrder -> 3];
{lo, hi} = {lnTs[[1]], lnTs[[-1]]};
SigmaPipe[T_?NumericQ] := If[lo <= Log[T] <= hi, SigPipeI[Log[T]], 0.];
gamPipe[T_?NumericQ] := gamPipeI[Clip[Log[T], {lo, hi}]];
fmPipe[T_?NumericQ] := Exp[lnfPipeI[Clip[Log[T], {lo, hi}]]];
(* the notebook's gamma, kept inline in the u equation (as in the notebook) so NDSolve preprocesses it *)
gamNBexpr[T_] := (T/3)*Abs[GstarsDerivInterp[T]/Gstarsinterp[T]];
SigmaNBraw[T_] := 1 - 3*NewOmegaFromData[T];
fmNB = fmTinterp;

v0 = 2.16*(1/(10^12*Mpl)); M = 10^(-2);
(* log-space forms of rhsPhi and PhiPrimeCoeff1, algebraically identical: numerator and denominator
   divided by rho, with V/rho formed from logarithms and set to 0 below e^-700, so that nothing leaves
   the machine range however large A^4 = exp(4 beta phi) is (variants with suffix "Log") *)
lnRho[x_?NumericQ, T_?NumericQ] := 4*\[Beta]*x - 4*Log[Mpl] + Log[(Pi^2/30)*Gstarrhointerp[T]*T^4];
Vr[x_?NumericQ, T_?NumericQ] := With[{a = 4*Log[v0] + M/x - lnRho[x, T]}, If[a < -700., 0., Exp[a]]];
rhsPhiLog[y_, x_, T_] := With[{vr = Vr[x, T], f = fmTinterp[T]},
   -3*(1 - y^2/6)*((-vr*M/x^2) + \[Beta]*(Sigma[T] + f))/((1 + f) + vr)];
coeffLog[y_, x_, T_] := With[{vr = Vr[x, T], f = fmTinterp[T]},
   (1 - y^2/6)*(1 + (4*vr + (Sigma[T] + f))/(2*((1 + f) + vr)))];

run[{b_, var_}] := Module[{sol, xs, ys, us, pend, grid, samp, tps, gamE, t, var0, logQ, yrhs},
  \[Beta] = b; \[Phi]i = phiInit;
  Clear[Sigma, fmTinterp];
  {var0, logQ} = If[StringEndsQ[var, "Log"], {StringDrop[var, -3], True}, {var, False}];
  Switch[var0,
    "nb", Sigma[T_] := SigmaNBraw[T]; fmTinterp = fmNB; gamE = gamNBexpr[Exp[u[p]]],
    "pipeSig", Sigma[T_?NumericQ] := SigmaPipe[T]; fmTinterp = fmNB; gamE = gamNBexpr[Exp[u[p]]],
    "common" | "commonV0", Sigma[T_?NumericQ] := SigmaPipe[T]; fmTinterp = fmPipe; gamE = gamPipe[Exp[u[p]]]];
  v0 = If[var === "commonV0", 0., 2.16*(1/(10^12*Mpl))];
  yrhs = If[logQ, rhsPhiLog[y[p], x[p], Exp[u[p]]] - coeffLog[y[p], x[p], Exp[u[p]]]*y[p],
    rhsPhi[y[p], x[p], Exp[u[p]]] - PhiPrimeCoeff1[\[Beta], x[p], Exp[u[p]], y[p], v0, M]*y[p]];
  t = First@AbsoluteTiming[
    sol = TimeConstrained[Quiet@NDSolve[{x'[p] == y[p],
        y'[p] == yrhs,
        u'[p] == -(1 + \[Beta]*y[p])/(1 + gamE),
        x[0] == \[Phi]i, y[0] == 0, u[0] == Log[2*10^4],
        WhenEvent[x[p] <= 0.1, "StopIntegration"]},
       {x, y, u}, {p, 0, Nmax}, Method -> {"StiffnessSwitching"},
       AccuracyGoal -> 10, PrecisionGoal -> 10, MaxStepSize -> maxStep], tlimit, $TimedOut]];
  If[sol === $TimedOut, Return[{b, var, t, "TIMED OUT", 0, {}}, Module]];
  {xs, ys, us} = {x, y, u} /. First[sol];
  pend = xs["Domain"][[1, 2]];
  grid = Range[0., pend, 0.001];
  samp = Transpose[{grid, xs[grid], ys[grid], us[grid]}];
  tps = Module[{g = 1 + b*samp[[All, 3]], idx},
    idx = Flatten@Position[Rest[g]*Most[g], _?Negative];
    Table[With[{p0 = q /. FindRoot[1 + b*ys[q] == 0, {q, samp[[i, 1]], samp[[i + 1, 1]]}, AccuracyGoal -> 13]},
        {p0, Exp[us[p0]], xs[p0], If[g[[i]] > 0, "min", "max"]}], {i, idx}]];
  Export[FileNameJoin[{outDir, StringJoin[tag, "_b", ToString[NumberForm[b, {digits + 1, digits}]], "_", var, ".csv"]}],
    Prepend[samp[[1 ;; -1 ;; 10]], {"N", "phi", "phiprime", "lnTJ"}]];
  {b, var, t, pend, Exp[us[pend]], tps}];

jobs = Tuples[{betas, variants}];
res = If[par,
  LaunchKernels[Min[Length[jobs], $ProcessorCount - 2]];
  DistributeDefinitions[run, SigmaPipe, gamPipe, fmPipe, gamNBexpr, SigmaNBraw, fmNB, SigPipeI, gamPipeI, lnfPipeI, lo, hi,
    rhsPhi, PhiPrimeCoeff1, rhsPhiLog, coeffLog, lnRho, Vr, RhoRadA, A, Vexp, VexpPrime, NewOmegaFromData, NewOmegaFromDataInterp,
    Gstarrhointerp, GstarsDerivInterp, Gstarsinterp, Mpl, M, phiInit, maxStep, Nmax, outDir, tag, digits, tlimit];
  ParallelMap[run, jobs, Method -> "FinestGrained"],
  Map[run, jobs]];

Do[Print[StringRiffle[ToString /@ {r[[1]], r[[2]], NumberForm[r[[3]], 4], NumberForm[r[[4]], 10], NumberForm[r[[5]], 7]}, " | "],
    "\n    TP: ", ToString[N[r[[6]], 10], InputForm]], {r, res}];
Export[FileNameJoin[{outDir, tag <> "_summary.m"}], res];
