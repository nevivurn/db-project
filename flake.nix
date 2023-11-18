{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, flake-utils, nixpkgs }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ self.overlays.default ];
          };
        in
        {
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              (python3.withPackages (ps: with ps; [ lark berkeleydb ]))
            ];
          };
        }
      )
    // {
      overlays.default = final: prev: {
        pythonPackagesExtensions = prev.pythonPackagesExtensions ++ [
          (py-final: py-prev: {
            berkeleydb = py-final.buildPythonPackage rec {
              pname = "berkeleydb";
              version = "18.1.8";

              src = final.fetchPypi {
                inherit pname version;
                hash = "sha256-4YMaeQv9hVdA5cEvlS8Z9kbCaYBs/DYnda/Zh4zzJVc=";
              };

              buildInputs = [ final.db ];
              env.BERKELEYDB_DIR = final.db.dev;
            };
          })
        ];
      };
    };
}
