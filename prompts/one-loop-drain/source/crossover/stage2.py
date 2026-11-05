"""
crossover side-campaign, stage 2: the one-loop scheme of wall_backreaction.tex (§"The Erickcek et al. scheme")
solved without the Born reduction, with the mass in the mode functions.

Units are walls.py's: M = 1, omega_t = 1, U = c(e^{1/phi} - 1) measured from the floor. In these units the
physical tadpole (1/2) V''' <dphi^2> becomes (1/2) V''' Sigma omega_t^2/M^2 with omega_t^2/M^2 = 8 pi^2 R E
(born.py's docstring), so

    phi'' = -U'(phi) - K V'''(phi) Sigma,        K = 4 pi^2 R E,
    E     = phi'^2/2 + U(phi) + 2K rho                                  (conserved),

where Sigma = (1/2 pi^2) int k^2 dk [|chi_k|^2 - 1/(2 omega_k)] and rho = (1/2 pi^2) int k^2 dk [eps_k], eps_k
the energy of mode k less the instantaneous vacuum's, both in units of omega_t, and omega_k^2 = k^2 + V''(phi(t)).
In the Born limit Sigma = -I/(8 pi^2), with I born.py's kernel integral, and the drag is born.py's +G V''' I with
G = R E / 2. The fraction of E carried by the fluctuations is 2K rho / E = 8 pi^2 R rho.

The modes, x'' + omega_k^2 x = 0 (the scheme's mode equation, eq:mode), are carried as Bogoliubov coefficients on
the instantaneous zeroth-order adiabatic basis,

    x  = (alpha e^{-i theta} + beta e^{i theta}) / sqrt(2 omega),
    x' = -i omega (alpha e^{-i theta} - beta e^{i theta}) / sqrt(2 omega),
    alpha' = (omega'/2 omega) beta e^{2 i theta},   beta' = (omega'/2 omega) alpha e^{-2 i theta},   theta' = omega,

so that |x|^2 - 1/(2 omega) = (|beta|^2 + Re[alpha beta* e^{-2 i theta}]) / omega and eps_k = omega |beta|^2
exactly. This is the subtraction the note uses (the static vacuum at the same phi), and it avoids the cancellation in
|x|^2 - 1/(2 omega): the occupations are 1e-13 to 1e-4 (stage 1b), far below what a direct integration of x resolves.

Each mode is started in the first-order adiabatic vacuum, alpha = 1 - i eta, beta = i eta, theta = 0, with
eta = omega'/(4 omega^2), when the earlier of two things happens: its adiabaticity |omega'|/omega^2 reaches EPS (the
soft modes, far out), or V'' reaches F0 m_ref^2 (every mode, before the pulse). EPS has to fall with the hierarchy
at large ell (run_point). Before then it is in that state to
first order and contributes nothing to Sigma; its omega eta^2 is added to the energy check. The background starts
where the lowest mode is adiabatic to EPS/2 on the incoming free trajectory.

The optional Hartree term (hartree=True) adds delta = (1/2) V''''(phi) Sigma to omega_k^2 in the mode equation
(K V'''' Sigma in these units, with the same normalization 2K as the drag),
the linearization of the background equation at fixed Sigma (the note's §"Internal consistency"), with Sigma
subtracted as above. It is not the full linearization (the response of Sigma is left out, the note's
§"Comparison with the figures of Erickcek et al."), and the energy above is not conserved with it.

Integration: classical fixed-order RK4, with the step h / max(2 omega_max over the started modes, |V'''phi'/V''|
+ sqrt(V''), ...) (h = 0.1 is about 600 steps per 1/k_ex with k_max = 30 k_ex).
"""

from math import ceil, exp, log, pi, sqrt

import numpy as np

import born
from walls import Wall, WallN

EPS = 1e-3
F0 = 1e-6


def V4(w: Wall, p):
    if isinstance(w, WallN):
        return w.V4(p)
    y = 1.0 / p
    return w.c * np.exp(y) * (y**8 + 12.0 * y**7 + 36.0 * y**6 + 24.0 * y**5)


def lam(w: Wall, p):
    """-d ln V/dphi on the full V = V0 + U."""
    return -w.dU(p) / (w.c + w.U(p))


def grid(kmin, kmax, ppd):
    """log-spaced k with trapezoid weights in ln k, folded with k^2 dk/(2 pi^2)."""
    n = max(int(ceil(log(kmax / kmin, 10.0) * ppd)) + 1, 8)
    k = np.geomspace(kmin, kmax, n)
    h = log(kmax / kmin) / (n - 1)
    wt = np.full(n, h)
    wt[0] = wt[-1] = 0.5 * h
    return k, k**3 * wt / (2.0 * pi**2)


def solve(
    L,
    lnR,
    kmin,
    kmax,
    ppd=80,
    mref=None,
    h=0.1,
    f0=F0,
    eps=EPS,
    loop=True,
    hartree=False,
    span=4.0,
    rec_every=20,
    n=1,
):
    w = Wall(L) if n == 1 else WallN(L, n)
    R = exp(lnR)
    E = w.E
    K = 4.0 * pi**2 * R * E if loop else 0.0
    K2 = (
        8.0 * pi**2 * R * E
    )  # rho -> energy (kept for the measurement when the loop is off)
    k, W = grid(kmin, kmax, ppd)
    k2 = k * k
    n = k.size
    if mref is None:
        mref = 1.0
    v2_entry = f0 * mref * mref

    def adiab_free(kk, p):
        return abs(w.V3(p) * w.phi_dot(p)) / (2.0 * (kk * kk + w.V2(p)) ** 1.5)

    # entry: where V'' = f0 mref^2 (outwards of the classical turning point); start: further out until the lowest
    # mode is adiabatic to eps/2 on the free incoming trajectory
    p = w.phi_t
    while w.V2(p) > v2_entry:
        p *= 1.2
    p_entry = p
    while adiab_free(k[0], p) > 0.5 * eps or w.V2(p) > v2_entry:
        p *= 1.2
        if p > 1e15:
            raise RuntimeError("no adiabatic start")
    phi, v = p, -float(w.phi_dot(p))
    a = np.ones(n, complex)
    b = np.zeros(n, complex)
    th = np.zeros(n)
    act = np.zeros(n, bool)

    def rhs(phi, v, a, b, th):
        V2, V3 = w.V2(phi), w.V3(phi)
        om = np.sqrt(k2 + V2)
        g = V3 * v / (2.0 * om) / (2.0 * om)
        e = np.exp(2j * th)
        ab = a * np.conj(b) * np.conj(e)
        X = (b.real**2 + b.imag**2 + ab.real) / om
        Sig = float(np.dot(W[act], X[act]))
        acc = -w.dU(phi) - K * V3 * Sig
        da = g * b * e
        db = g * a * np.conj(e)
        if hartree:
            d = K * V4(w, phi) * Sig / (2.0 * om)
            be = b * e
            aec = a * np.conj(e)
            da = da - 1j * d * (a + be)
            db = db + 1j * d * (b + aec)
        da[~act] = 0.0
        db[~act] = 0.0
        return v, acc, da, db, om, Sig

    def energy(phi, v, a, b):
        V2, V3 = w.V2(phi), w.V3(phi)
        om = np.sqrt(k2 + V2)
        eta = V3 * v / (2.0 * om) / (4.0 * om * om)
        nb = np.where(act, b.real**2 + b.imag**2, eta * eta)
        rho = float(np.dot(W, om * nb))
        return 0.5 * v * v + w.U(phi), rho

    t = 0.0
    rec = []
    t_entry = t_turn = None
    phi_turn = None
    sig_turn = 0.0
    turned = returned = False
    v_in = abs(v)
    vmin_phi = phi
    sig_max = 0.0
    drift = 0.0
    step = 0
    while True:
        V2 = w.V2(phi)
        V3 = w.V3(phi)
        # start the modes due now
        if not act.all():
            om = np.sqrt(k2 + V2)
            ad = np.abs(V3 * v) / (2.0 * om**3)
            due = (~act) & ((ad >= eps) | (V2 >= v2_entry))
            if due.any():
                eta = V3 * v / (2.0 * om[due]) / (4.0 * om[due] ** 2)
                a[due] = 1.0 - 1j * eta
                b[due] = 1j * eta
                th[due] = 0.0
                act |= due
        if t_entry is None and V2 >= v2_entry:
            t_entry = t
        om_max = sqrt(k2[act].max() + V2) if act.any() else 0.0
        rate = abs(V3 * v / V2) + sqrt(V2)
        dt = h / max(2.0 * om_max, rate, 1e-300)
        # RK4
        v1, a1, da1, db1, om1, S1 = rhs(phi, v, a, b, th)
        v2_, a2, da2, db2, om2, _ = rhs(
            phi + 0.5 * dt * v1,
            v + 0.5 * dt * a1,
            a + 0.5 * dt * da1,
            b + 0.5 * dt * db1,
            th + 0.5 * dt * om1,
        )
        v3_, a3, da3, db3, om3, _ = rhs(
            phi + 0.5 * dt * v2_,
            v + 0.5 * dt * a2,
            a + 0.5 * dt * da2,
            b + 0.5 * dt * db2,
            th + 0.5 * dt * om2,
        )
        v4_, a4, da4, db4, om4, _ = rhs(
            phi + dt * v3_, v + dt * a3, a + dt * da3, b + dt * db3, th + dt * om3
        )
        phi_new = phi + dt * (v1 + 2 * v2_ + 2 * v3_ + v4_) / 6.0
        v_new = v + dt * (a1 + 2 * a2 + 2 * a3 + a4) / 6.0
        a = a + dt * (da1 + 2 * da2 + 2 * da3 + da4) / 6.0
        b = b + dt * (db1 + 2 * db2 + 2 * db3 + db4) / 6.0
        th = th + dt * (om1 + 2 * om2 + 2 * om3 + om4) / 6.0
        if phi_new <= 0.0:
            raise RuntimeError("field reached phi <= 0")
        if not turned and v_new > 0.0:
            turned = True
            t_turn = t + dt
            phi_turn = min(phi, phi_new)
            sig_turn = S1
        phi, v = phi_new, v_new
        t += dt
        step += 1
        sig_max = max(sig_max, abs(S1))
        vmin_phi = min(vmin_phi, phi)
        if step % rec_every == 0 or turned and returned:
            ekin, rho = energy(phi, v, a, b)
            dE = (ekin + K2 * rho - E) / E if loop else 0.0
            drift = max(drift, abs(dE))
            rec.append((t, phi, v, S1, K2 * rho / E, dE))
        if turned and v > 0.0 and phi >= p_entry:
            returned = True
            break
        if turned and t > t_turn + span * (t_turn - (t_entry or 0.0)):
            break
        if step > 5_000_000:
            raise RuntimeError("step budget")
    ekin, rho = energy(phi, v, a, b)
    om = np.sqrt(k2 + w.V2(phi))
    nb = b.real**2 + b.imag**2
    dE = (ekin + K2 * rho - E) / E if loop else 0.0
    ch = born.chain(w, R)
    lam_t = float(lam(w, phi_turn))
    return dict(
        L=L,
        lnR=lnR,
        E=E,
        chain=(
            None if ch is None else dict(phi_ta=ch[0], m_ta=ch[1], kex=ch[2], ell=ch[3])
        ),
        k=k,
        W=W,
        n=nb,
        om=om,
        frac_modes=8.0 * pi**2 * R * rho,
        frac_balance=(E - ekin) / E,
        vout=v / v_in,
        returned=returned,
        phi_end=phi,
        v2_end_rel=float(w.V2(phi) / w.V2(phi_turn)),
        phi_turn=phi_turn,
        m_turn=sqrt(w.V2(phi_turn)),
        kex_turn=lam_t * sqrt(E) / 2.0,
        eps_turn=0.5 * R * E * float(V4(w, phi_turn)),
        hartree_turn=4.0 * pi**2 * R * E * float(V4(w, phi_turn)) * sig_turn,
        sig_max=sig_max,
        drift=max(drift, abs(dE)),
        steps=step,
        nk=k.size,
        rec=np.array(rec),
    )


def run_point(L, lnR, cmin=0.03, cmax=30.0, ppd=80, iterate=True, n=1, **kw):
    """solve() on a grid from cmin m_ref to cmax k_ref, with m_ref, k_ref the chain's m_ta, k_ex where it exists,
    else the classical wall's (omega_t = 1, lambda_t sqrt(E)/2); then, if the field turned at a mass or a crossing
    rate (lambda sqrt(E)/2 at the turn) off the reference by more than a factor 1.5, again with the grid set from the
    turn (at most three passes).

    Above ell = 8 the start threshold EPS falls as e^{-2(ell - 8)} unless eps is given. A mode started in the
    first-order state carries a second-order mismatch of order EPS^2 in beta, and its energy against E grows as the
    hierarchy e^{4 ell}: at ell = 17.75 the modes' energy is off E's balance by 1.6e-2 at EPS = 1e-3 and by 1.6e-6
    at 1e-9, while the background's balance is unchanged to five digits (out/stage2_validation.txt, V5).
    """
    w = Wall(L) if n == 1 else WallN(L, n)
    ch = born.chain(w, exp(lnR))
    if ch is not None and ch[3] > 8.0 and "eps" not in kw:
        kw = dict(kw, eps=EPS * exp(-2.0 * (ch[3] - 8.0)))
    if ch is not None:
        mref, kref = ch[1], ch[2]
    else:
        mref, kref = 1.0, float(lam(w, w.phi_t)) * sqrt(w.E) / 2.0
    for it in range(3):
        res = solve(
            L, lnR, kmin=cmin * mref, kmax=cmax * kref, ppd=ppd, mref=mref, n=n, **kw
        )
        res["mref"], res["kref"], res["passes"] = mref, kref, it + 1
        if not iterate:
            break
        mt, kt = res["m_turn"], res["kex_turn"]
        if 1 / 1.5 <= mt / mref <= 1.5 and 1 / 1.5 <= kt / kref <= 1.5:
            break
        mref, kref = mt, kt
    return res
