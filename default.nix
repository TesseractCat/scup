{ pkgs ? import <nixpkgs> {} }:

pkgs.rustPlatform.buildRustPackage {
  pname = "scup";
  version = "0.1.0";

  src = ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  nativeBuildInputs = with pkgs; [
    pkg-config
  ];

  buildInputs = with pkgs; [
    openssl
  ];

  meta = with pkgs.lib; {
    description = "Git-like history with Syncthing-like automatic synchronization for large files";
    license = licenses.gpl3Only;
    platforms = platforms.linux ++ platforms.darwin;
    mainProgram = "scup";
  };
}
