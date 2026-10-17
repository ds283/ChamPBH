(* Symbolic checks of the field equations (M_P = 1). rho = Einstein-frame radiation density, f = rho_m/rho,
   S = Sigma = 1 - 3w, V, Vp = V', y = dphi/dN, b = beta.  No notebook needed. *)
w = (1 - S)/3;
threeH2 = (rho (1 + f) + V)/(1 - y^2/6);                         (* Friedmann *)
H2 = threeH2/3;
(* Raychaudhuri: Hdot = -(rho_tot + p_tot + phidot^2)/2, phidot^2 = H^2 y^2 *)
HdotOverH2 = -(rho (1 + w) + rho f + H2 y^2)/(2 H2);
(* phi'' = -(3 + Hdot/H^2) phi' - (V' + b rho (S + f))/H^2  from  phiddot + 3 H phidot + V' = -b (rho - 3 p + rho_m) *)
exact = -(3 + HdotOverH2) y - (Vp + b rho (S + f))/H2;
(* notebook: rhsPhi - PhiPrimeCoeff1 y (cell 3) *)
nb = -3 (1 - y^2/6) (Vp + b rho (S + f))/(rho (1 + f) + V) - (1 - y^2/6) (1 + (4 V + (S + f) rho)/(2 (rho (1 + f) + V))) y;
(* pipeline: friction + reflecting + kicking (ODEPolicy, PotentialDerivativePolicy) *)
G = 1 - y^2/6; R = (S + f)/(1 + f); cc = (V/(3 H2))/2;   (* G, R, C of Hdot_over_H2_plus_3 *)
pipe = -y (G (1 + R/2) + cc (4 - R)) - 3 Vp/(3 H2) - 3 (G - V/(3 H2)) b R;
Print["exact - notebook  : ", Simplify[exact - nb]];
Print["exact - pipeline  : ", Simplify[exact - pipe]];
(* radiation, matter fraction and temperature: the pipeline's d ln rho_E/dN and d ln f/dN from Jordan-frame conservation,
   with N_J = N + ln A, d ln A/dN = b y, rho_E = A^4 rho_J, rho_J ~ a_J^(-3(1+w)), rho_m,J ~ a_J^-3 *)
dlnAJ = 1 + b y;
Print["d ln rho_E/dN    : ", Simplify[(-3 (1 + w) dlnAJ + 4 b y) - (S - 4 + S b y)]];
Print["d ln f_m/dN      : ", Simplify[(-3 + 3 (1 + w)) dlnAJ - (1 - S) (1 + b y)]];
