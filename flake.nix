{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, flake-utils, nixpkgs }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.packages.${system}.default ];
          nativeBuildInputs = with pkgs; [ mysql ];
        };
        packages.default = pkgs.python3Packages.buildPythonApplication {
          name = "db-proj2";
          src = ./.;
          format = "other";

          nativeBuildInputs = [ pkgs.python3Packages.mypy ];
          propagatedBuildInputs = [ pkgs.python3Packages.mysql-connector ];

          installPhase = ''
            runHook preInstall
            install -Dm755 run.py $out/bin/run.py
            runHook postInstall
          '';

          meta.mainProgram = "run.py";
        };
      }
    );
}
