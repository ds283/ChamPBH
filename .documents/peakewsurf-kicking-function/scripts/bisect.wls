(* Bisect the EW-capture threshold in beta: captured = a T_J minimum above 20 GeV.
   Args as swap_runs.wls, but arg 5 = "{lo,hi}" and arg 11 = tolerance. *)
Get[FileNameJoin[{DirectoryName[$ScriptCommandLine[[1]]], "swap_defs.wl"}]];
{blo, bhi} = betas; tol = ToExpression[args[[11]]];
captured[b_, var_] := Module[{r = run[{b, var}]},
  Print["   beta=", NumberForm[b, 8], " ", var, " TPs=", N[r[[6]], 5]];
  AnyTrue[r[[6]], #[[4]] == "min" && #[[2]] > 20 &]];
Do[Module[{lo = blo, hi = bhi, mid},
   If[captured[lo, var] || ! captured[hi, var], Print[var, ": not bracketed"]; Continue[]];
   While[hi - lo > tol, mid = (lo + hi)/2; If[captured[mid, var], hi = mid, lo = mid]];
   Print["### ", var, ": EW-capture threshold in (", NumberForm[lo, 8], ", ", NumberForm[hi, 8], ")"]],
  {var, variants}];
