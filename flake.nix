{
  description = "Development shell for nekroddos";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, rust-overlay, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          rustToolchain = pkgs.rust-bin.stable."1.91.0".default.override {
            extensions = [
              "clippy"
              "rustfmt"
              "rust-src"
            ];
          };
          pythonEnv = pkgs.python3.withPackages (
            ps: with ps; [
              ipython
              matplotlib
              pandas
              seaborn
            ]
          );
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              git
              openssl
              pkg-config
              pythonEnv
              rust-analyzer
              rustToolchain
              uv
            ];

            PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";

            shellHook = ''
              rustc_ver="$(rustc --version | awk '{print $2}')"
              if [ "$rustc_ver" != "1.91.0" ]; then
                echo "ERROR: expected rustc 1.91.0, got $rustc_ver" >&2
                exit 1
              fi

              echo "nekroddos dev shell active (rustc $rustc_ver)"
            '';
          };
        }
      );
    };
}
