(* Run PeakEWsurf.nb's NDSolve with Sigma(T) swapped.
   Args: notebook path, scratch dir, pipeline Sigma CSV, beta list (e.g. "{3.0,3.01}"),
         variants (e.g. "{\"nb\",\"pipe\",\"ew\"}"), MaxStepSize, phi_i, tag, parallel (True/False). *)
args = $ScriptCommandLine;
{nbPath, outDir, sigCSV} = args[[2 ;; 4]];
betas = ToExpression[args[[5]]];
variants = ToExpression[args[[6]]];
maxStep = ToExpression[args[[7]]];
phiInit = ToExpression[args[[8]]];
tag = args[[9]];
par = ToExpression[args[[10]]];

nb = Import[nbPath, "NB"];
inputs = Cases[nb, Cell[c_, "Input", ___] :> c, Infinity];
Quiet[ToExpression[inputs[[1]], StandardForm]];   (* g* fits *)
Quiet[ToExpression[inputs[[2]], StandardForm]];   (* thermo: w table, Sigma, fm, g* interps *)
Quiet[ToExpression[inputs[[3]], StandardForm]];   (* rhsPhi, PhiPrimeCoeff1 *)

(* the notebook's own Sigma, and the pipeline's (Xav_EOS_spline.w, sampled at 1000/decade) *)
SigmaNB[T_] := 1 - 3*NewOmegaFromData[T];
sig = Rest@Import[sigCSV, "CSV"];
SigmaPipeInterp = Interpolation[{Log[#[[1]]], #[[2]]} & /@ sig, InterpolationOrder -> 3];
{lnTlo, lnThi} = {Log[sig[[1, 1]]], Log[sig[[-1, 1]]]};
SigmaPipe[T_] := If[lnTlo <= Log[T] <= lnThi, SigmaPipeInterp[Log[T]], 0.];
TSWITCH = 20.;
SigmaEW[T_] := If[T < TSWITCH, SigmaNB[T], SigmaPipe[T]];

sigmaOf["nb"] = SigmaNB; sigmaOf["pipe"] = SigmaPipe; sigmaOf["ew"] = SigmaEW;

(* the notebook's parameters (cells 7, 13) *)
v0 = 2.16*(1/(10^12*Mpl)); M = 10^(-2);

run[{b_, var_}] := Module[{sol, xs, ys, us, pend, t, grid, samp, tps, s},
  \[Beta] = b; \[Phi]i = phiInit;
  Clear[Sigma];
  If[var === "raw", Sigma[T_] := 1 - 3*NewOmegaFromData[T],
    s = sigmaOf[var]; Sigma[T_?NumericQ] := s[T]];
  t = First@AbsoluteTiming[
    sol = Quiet@NDSolve[{x'[p] == y[p],
        y'[p] == rhsPhi[y[p], x[p], Exp[u[p]]] - PhiPrimeCoeff1[\[Beta], x[p], Exp[u[p]], y[p], v0, M]*y[p],
        u'[p] == -(1 + \[Beta]*y[p])/(1 + (Exp[u[p]]/3)*Abs[GstarsDerivInterp[Exp[u[p]]]/Gstarsinterp[Exp[u[p]]]]),
        x[0] == \[Phi]i, y[0] == 0, u[0] == Log[2*10^4],
        WhenEvent[x[p] <= 0.1, "StopIntegration"]},
       {x, y, u}, {p, 0, 150}, Method -> {"StiffnessSwitching"},
       AccuracyGoal -> 10, PrecisionGoal -> 10, MaxStepSize -> maxStep]];
  {xs, ys, us} = {x, y, u} /. First[sol];
  pend = xs["Domain"][[1, 2]];
  grid = Range[0., pend, 0.001];
  samp = Transpose[{grid, xs[grid], ys[grid], us[grid]}];
  (* T_J turning points: zeros of 1 + beta y *)
  tps = Module[{g = 1 + b*samp[[All, 3]], idx},
    idx = Flatten@Position[Rest[g]*Most[g], _?Negative];
    Table[With[{p0 = FindRoot[1 + b*ys[q] == 0, {q, samp[[i, 1]], samp[[i + 1, 1]]}][[1, 2]]},
        {p0, Exp[us[p0]], xs[p0], If[g[[i]] > 0, "min", "max"]}], {i, idx}]];
  Export[FileNameJoin[{outDir, StringJoin[tag, "_b", ToString[NumberForm[b, {5, 3}]], "_", var, ".csv"]}],
    Prepend[samp[[1 ;; -1 ;; 10]], {"N", "phi", "phiprime", "lnTJ"}]];
  {b, var, t, pend, Exp[us[pend]], tps}];


(* the pipeline's CSV rows, interpolated the notebook's way (log-log Interpolation, default order 3),
   with w = 1/3 outside the CSV's range as Xav_EOS_spline does *)
csvRows = N@Rest@Import["CosmologyModels/GenericEOS/Xav_EOS_data.csv", "CSV"];
csvInterp = Exp @* Interpolation[Log[csvRows]] @* Log;
{csvTlo, csvThi} = {csvRows[[1, 1]], csvRows[[-1, 1]]};
SigmaCSVnb[T_] := If[csvTlo < T < csvThi, 1 - 3*csvInterp[T], 0.];
sigmaOf["csvnb"] = SigmaCSVnb;
