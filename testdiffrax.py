from diffrax import diffeqsolve, Kvaerno5, ODETerm, SaveAt, PIDController

vector_field = lambda t, y, args: (-5 * y[0], 0.5 * y[1])
term = ODETerm(vector_field)
solver = Kvaerno5()
saveat = SaveAt(dense=True)
stepsize_controller = PIDController(rtol=1e-5, atol=1e-5)

sol = diffeqsolve(
    term,
    solver,
    t0=0,
    t1=3,
    dt0=0.1,
    y0=(1, 1),
    saveat=saveat,
    stepsize_controller=stepsize_controller,
)

print(sol)
