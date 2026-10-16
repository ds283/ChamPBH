Get[FileNameJoin[{DirectoryName[$ScriptCommandLine[[1]]], "swap_defs.wl"}]];
grid = Table[10.^i, {i, -5., Log10[2.*10^4], 1/1000.}];
Export[FileNameJoin[{outDir, "sigma_csv_nbscheme.csv"}],
  Prepend[Table[{T, SigmaCSVnb[T]}, {T, grid}], {"T_GeV", "Sigma_csvnb"}]];
Print["done"];
