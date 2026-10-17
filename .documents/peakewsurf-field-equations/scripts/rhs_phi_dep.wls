nb = Import[$ScriptCommandLine[[2]], "NB"];
inputs = Cases[nb, Cell[c_, "Input", ___] :> c, Infinity];
Quiet[ToExpression[inputs[[#]], StandardForm]] & /@ {1, 2, 3};
v0 = 2.16*(1/(10^12*Mpl)); M = 10^(-2); \[Beta] = 3.005;
Do[With[{r10 = rhsPhi[y, 10., T], r100 = rhsPhi[y, 100., T], c10 = PhiPrimeCoeff1[\[Beta], 10., T, y, v0, M], c100 = PhiPrimeCoeff1[\[Beta], 100., T, y, v0, M]},
   Print["y=", y, " T=", T, "  rhsPhi(10)=", InputForm[r10], " rhsPhi(100)=", InputForm[r100], "  coeff(10)=", InputForm[c10], " coeff(100)=", InputForm[c100],
     "  rho(100)=", InputForm[RhoRadA[\[Beta], 100., T]], " Precision=", Precision[RhoRadA[\[Beta], 100., T]]]],
  {y, {-0.01, -0.2}}, {T, {200., 80.}}];
Print["Exp[4*300.5] machine: ", InputForm[Exp[4*300.5]], "  $MaxMachineNumber=", $MaxMachineNumber];
Print["rho*Sigma/rho: ", InputForm[With[{r = RhoRadA[\[Beta], 100., 80.]}, r*0.03/(r*(1 + 1.*^-11) + 1.*^-120)]]];
(* the same derivative evaluated through NDSolve's compiled path: a one-step NDSolve from phi=100 vs 10 *)
f[x0_] := Module[{s}, s = Quiet@NDSolve[{x'[p] == y[p], y'[p] == rhsPhi[y[p], x[p], Exp[u[p]]] - PhiPrimeCoeff1[\[Beta], x[p], Exp[u[p]], y[p], v0, M]*y[p],
     u'[p] == -(1 + \[Beta]*y[p])/(1 + (Exp[u[p]]/3)*Abs[GstarsDerivInterp[Exp[u[p]]]/Gstarsinterp[Exp[u[p]]]]), x[0] == x0, y[0] == -0.05, u[0] == Log[200.]},
     {x, y, u}, {p, 0, 1}, Method -> {"StiffnessSwitching"}, AccuracyGoal -> 10, PrecisionGoal -> 10, MaxStepSize -> 10^-3];
   {y[1], u[1]} /. First[s]];
Print["1 e-fold from y=-0.05, T=200: phi0=10 -> ", InputForm[f[10.]], "  phi0=100 -> ", InputForm[f[100.]], " phi0=50 -> ", InputForm[f[50.]]];
