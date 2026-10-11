# Brief: a `v0.2.3` release whose factory base does not refuse instances

**For:** the agent that plans a campaign in DatastoreKit (`/Users/ds283/Documents/Code/DatastoreKit`)
to release `datastorekit` **`v0.2.3`**, under DatastoreKit's `CLAUDE.md` and its campaign
conventions. **Written:** 2026-10-11, from a Claude Code session in ChamPBH, with the user, while
planning ChamPBH's adoption campaign (`prompts/dk-and-rpk-migration/` in ChamPBH, README §6.1, U3).
**Trees:** DatastoreKit at `460b012` (`v0.2.2` is `6d8ee9a`); ChamPBH at `52142d7`.

**This brief is evidence to check, not a specification** (DatastoreKit `CLAUDE.md` invariant 7).
The user's ruling, in §1, is a ruling. Everything else is a finding with its citation, or a
recommendation marked **Advice:**. Where this brief and DatastoreKit's own records disagree, the
records are right, and the disagreement is worth reporting back. The campaign name, the decision
numbers and the shape of the campaign are DatastoreKit's to choose.

## 1. What is asked, and who ruled it

**The user's ruling (2026-10-11):** fix this in DatastoreKit, not in the clients. ChamPBH keeps
registering factory instances, and pins a release in which that works.

**The change asked for.** In `datastorekit/SQL/factory_base.py`, `SQLAFactoryBase` keeps
`register` and `build` abstract, and gives `store`, `validate` and `validate_on_startup` **non-abstract
defaults that raise `NotImplementedError`**, as `revalidate` already is. Nothing else in the layer's
behaviour changes. It is released as `v0.2.3`, a tag on DatastoreKit's remote, by a prompt that says
so (DatastoreKit `CLAUDE.md`, "Releases").

This reverses DatastoreKit's decision **U32** (2026-10-09), in part. U32 is DatastoreKit's: the
campaign records why it is revisited, in DatastoreKit's own decision table.

## 2. The defect

**The layer is indifferent to classes or instances. The base class is not.**
- The layer never instantiates a factory. It calls each hook on whatever the registry maps a class
  name to: `factory.register()` (`datastorekit/SQL/schema.py:99` at `v0.2.2`), `factory.build(...)`,
  `factory.store(...)`, `factory.validate(...)`, `factory.validate_on_startup(...)` and the rest
  (`datastorekit/SQL/Datastore.py:266`, `:492`, `:565`, `:711`, `:876`; `SQL/ShardedPool.py:2110`,
  `:2289`, `:2324-2325`, `:3490`, `:3499`). The contract says so (`docs/client-contract.md` §3;
  adoption README §4, "The layer never instantiates a factory"). A class with static hooks and an
  instance with bound hooks are called the same way.
- `SQLAFactoryBase` (`datastorekit/SQL/factory_base.py:5-29`) is an `ABC` whose five hooks are all
  `@staticmethod @abstractmethod`. Python enforces abstract methods **only when a class is
  instantiated**.
- So the ABC **checks nothing for a client that registers classes** (SGK, and the neutral test
  client): its classes are never instantiated. And it **refuses valid factories for a client that
  registers instances** (ChamPBH, StochasticInstantons): every instantiated class must define
  `store`, `validate` and `validate_on_startup`, including the many that the layer will never call
  them on.
- **When the layer calls the three.** `store` only on an `object_store` of that class; `validate`
  only on an `object_validate`; `validate_on_startup` only for a class whose `register()` says
  `validate_on_startup: True` (contract §3, §2). A parameter table such as `redshift` or `M_value`
  meets none of them.

**The contract already treats the three as optional.**
- Contract §3, `store`'s row: "Not defined: the base's `NotImplementedError`". That is the
  behaviour of a defaulted hook, and it is what the ABC prevents an instance from reaching.
- The base already defaults `revalidate` (raises), `owned_serials` (`None`) and `inventory_spec`
  (`None`), with the comment "They are not abstract, so a factory that has no use for one need not
  define it" (`factory_base.py`, after `:29`). The three hooks asked about are in the same position.
- `docs/extraction-verification.md` records 07a's observation §7.3, "contract §3 does not say what
  U32 found about instances", as still open. Contract §3's first sentence, "A factory is a class
  whose static methods the layer calls; it is never instantiated", is not true of what the layer
  accepts.

**Where the ABC came from.** It is SGK's base, carried over at extraction (`PROVENANCE.md`: SGK's
`Datastore/SQL/ObjectFactories/base.py` → `datastorekit/SQL/factory_base.py`). No DatastoreKit
decision chose it as a requirement on clients; U32 met its consequence at 07a and kept it.

**Why the clients register instances.** ChamPBH began as a copy of SGK's layer. At its initial
commit `dec2d45` (2025-12-09) its base was SGK's ABC and its registry named classes. Its commit
`5ef07b4` (2026-01-05) added two quantity factories, each one class serving several tables told
apart by an `ObjectType` held on the instance (`sqla_dimensionful_quantity_factory(M_value)`,
`(Lambda_value)`, `(temperature)`, `(phi_value)`, `(pi_value)`; `sqla_dimensionless_quantity_factory(beta_value)`).
To do that it made its base a plain class whose hooks raise `NotImplementedError`, removed
`@staticmethod` from every hook, and registered instances throughout. StochasticInstantons
descends from the same line. A parameterised factory is a natural use of an instance, and nothing
in the layer forbids it.

**What U32 asked of the clients instead**, from the checklists:
- ChamPBH: 20 classes over 24 registry entries (`docs/adoption/champbh.md` item 5, "U32").
- StochasticInstantons: 16 classes over 18 entries (`docs/adoption/stochasticinstantons.md` item 5).
- Either the three hooks written on every such class, or every hook made callable without an
  instance and the classes registered. The second is ChamPBH's 23 classes rewritten (about 60 hooks
  made static, six quantity subclasses), to satisfy a check that protects nothing.

**U32's stated reason for rejecting the package fix** was that it "changes the contract that SGK's
factories and the neutral client satisfy". The change only **relaxes** the base: every factory that
satisfies the `v0.2.2` base satisfies the `v0.2.3` one. SGK's factories and the neutral client are
classes, never instantiated, and see no difference.

## 3. What to check before planning

These are the brief's findings, to be measured again at DatastoreKit's `HEAD`:
1. **No test depends on the three hooks being abstract.** `git grep` at `v0.2.2` over
   `datastorekit/tests` finds no test that instantiates a factory to see a `TypeError`, and no
   `__isabstractmethod__` check. Two tests read the base's hooks directly:
   - `test_declared_facts.py:542` and `:563` call `SQLAFactoryBase.revalidate` and
     `.owned_serials` on the class;
   - `test_neutral_client.py:124-130`, `_defines(factory, hook)`, compares a factory's hook with
     `getattr(SQLAFactoryBase, hook)` by identity, to tell an own hook from the base's. A
     non-abstract `@staticmethod` default is still one function object, reached the same way from
     a class or an instance, so `_defines` should be unaffected. Measure it.
2. **Nothing in the layer relies on the hooks being static.** The brief found every call made on
   the registry entry (§2). Check for any call on the base or on `type(factory)`, any `inspect` of
   a hook, and any place that pickles a factory in a way an instance would not survive. An instance
   of a module-level class whose attributes are classes or plain values pickles under Ray as a
   class does (contract §6, "Picklable, defined at module level").
3. **The defaults' form.** A `@staticmethod` default raising `NotImplementedError` serves both a
   class and an instance, since a static method is called with the same arguments from either.
   **Advice:** keep them static, with the same signatures as today's abstract ones, so that the
   message, the signature and `_defines` are as they are; the actor already names the class when it
   reports a failed hook. Whether the message should name the hook is DatastoreKit's choice.

## 4. What a release would carry

**Advice**, for DatastoreKit to adopt or change:

- **The code.** `factory_base.py` only. `register` and `build` stay `@staticmethod @abstractmethod`;
  `store`, `validate` and `validate_on_startup` lose `@abstractmethod` and move under the existing
  comment on defaulted hooks.
- **The tests**, in DatastoreKit's suite (no client is imported):
  1. a factory **instance** whose class defines only `register` and `build` instantiates, and works
     through the stand-in pool: a get stores a row, and a reopen finds it;
  2. a parameterised factory, two instances of one class over two tables differing by a constructor
     argument, each building and reading back its own rows (ChamPBH's quantity factories in
     miniature, with neutral names);
  3. a class still cannot be instantiated without `register` or without `build` (`TypeError`);
  4. calling `store`, `validate` or `validate_on_startup` where a factory defines none raises
     `NotImplementedError`, from a class and from an instance;
  5. the neutral client, which registers classes, is unchanged: its tests pass as they stand.
- **The documents.**
  - `docs/client-contract.md`: a §9 subsection for the change, with its supersession of §3's first
    sentence and its rows for the three hooks (kind: "defaulted: raises `NotImplementedError`"),
    and closing 07a's observation §7.3. §1–§9.4 stay as written (`CLAUDE.md` invariant 6).
  - `docs/adoption/README.md`: a `v0.2.3` addendum, superseding §4's paragraph on instances.
  - `docs/adoption/champbh.md` and `docs/adoption/stochasticinstantons.md`: a `v0.2.3` addendum
    each, superseding item 5's U32 paragraph: with `v0.2.3`, the client's registry of instances
    works as it stands, and item 5 asks nothing of its factories but the base's import path.
  - `docs/adoption/secondarygwkit.md`: an addendum saying nothing changes for SGK.
  - `PROVENANCE.md`, `README.md` ("Status", "Changes"), the version in `pyproject.toml`, and
    `docs/extraction-verification.md` or the campaign's own verification record, as DatastoreKit's
    release prompts have done for `v0.2.1` and `v0.2.2`.
- **The release.** CI green at both tested ends, then the tag `v0.2.3` pushed to the remote, as for
  `v0.2.2` (the `actor-names` campaign, prompt 02).

## 5. What each client does with it

- **ChamPBH** pins `v0.2.3` in its adoption campaign (its prompt 07). It changes no factory but the
  base's import path. Its prompts 01–06 do not wait for the release; 07–10 do.
- **StochasticInstantons** adopts `v0.2.3` when it is next active, and so needs no U32 change either.
- **SGK** is mid-way through its own adoption (`SecondaryGWKit/prompts/dk-and-rpk-migration/`),
  with `v0.2.2` pinned and installed. Nothing in `v0.2.3` reaches it. **Advice:** say in the
  addendum that SGK may stay on `v0.2.2` or move in a commit of its own, as it chooses; do not
  require it, since `v0.2.3` fixes nothing SGK meets.

## 6. Rules that come with this request

- **No change in a client.** The campaign reads ChamPBH, SGK and StochasticInstantons through `git`
  only, as the checklists were measured. It edits none of them.
- **Nothing the layer writes changes**, and no message, refusal or exception changes but the one
  this release removes: the `TypeError` on instantiating a factory class that lacks `store`,
  `validate` or `validate_on_startup`.
- **Nothing else rides in the release.** A defect found while doing this is recorded and opened as
  an issue on DatastoreKit's board, not fixed in `v0.2.3`, unless the user rules otherwise.
- **The gates G2–G4 are unchanged in kind.** G3 and G4 read "has adopted `v0.2.3`" once it exists;
  G2 may read either release.

## 7. First steps

1. Re-measure §3's three points at DatastoreKit's `HEAD`.
2. Read DatastoreKit's U32 (`prompts/extraction/README.md` §6.2, and the board's row), the contract
   §3 and §9, `factory_base.py`, and item 5 of the ChamPBH and StochasticInstantons checklists.
3. Plan the campaign under DatastoreKit's conventions: decisions recorded, one commit per prompt,
   the tag made by a prompt that says so.
4. When `v0.2.3` is tagged, tell the user the tag object and commit. ChamPBH's prompt 07 is written
   from them.
