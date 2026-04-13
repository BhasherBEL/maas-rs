{
  description = "maas-rs — multi-modal MaaS routing engine (A* + RAPTOR)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      # Standalone package build — reused by eachDefaultSystem and the NixOS module.
      mkPackage = pkgs: pkgs.rustPlatform.buildRustPackage {
        pname = "maas-rs";
        version = "0.0.1";
        src = ./.;
        cargoLock.lockFile = ./Cargo.lock;

        nativeBuildInputs = [ pkgs.pkg-config ];
        # openssl-sys is a transitive dependency (via poem/hyper-tls).
        buildInputs = [ pkgs.openssl ];

        meta = with pkgs.lib; {
          description = "Multi-modal MaaS routing engine (A* + RAPTOR) over OSM + GTFS";
          license = licenses.mit;
          mainProgram = "maas-rs";
        };
      };
    in

    # ── Per-system outputs (packages, devShell) ───────────────────────────────
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages.default = mkPackage pkgs;

        devShells.default = pkgs.mkShell {
          # Inherit all build inputs from the package.
          inputsFrom = [ self.packages.${system}.default ];
          packages = with pkgs; [ rust-analyzer rustfmt clippy ];
          # Replicate .envrc: expose openssl for cargo build outside nix.
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
        };
      }
    )

    # ── System-independent outputs (NixOS module) ─────────────────────────────
    // {
      nixosModules.default = { config, lib, pkgs, ... }:
        let
          cfg = config.services.maas-rs;
          yamlFormat = pkgs.formats.yaml { };

          configFile = pkgs.writeText "maas-rs.yaml" (builtins.toJSON cfg.settings);

          startArgs = {
            "restore-and-serve"    = "--restore --serve";
            "build-and-serve"      = "--build --serve";
            "update-gtfs-and-serve" = "--update-gtfs --serve";
          }.${cfg.mode};
        in
        {
          # ── Options ──────────────────────────────────────────────────────────
          options.services.maas-rs = {
            enable = lib.mkEnableOption "maas-rs multi-modal routing engine";

            package = lib.mkOption {
              type = lib.types.package;
              default = mkPackage pkgs;
              defaultText = lib.literalExpression "maas-rs.packages.\${system}.default";
              description = "The maas-rs package to use.";
            };

            dataDir = lib.mkOption {
              type = lib.types.str;
              default = "/var/lib/maas-rs";
              description = ''
                Working directory for the service. graph.bin, osm.bin, and any
                data files referenced by relative `path:` URLs in
                `settings.build.inputs` must live here.
              '';
            };

            mode = lib.mkOption {
              type = lib.types.enum [
                "restore-and-serve"
                "build-and-serve"
                "update-gtfs-and-serve"
              ];
              default = "restore-and-serve";
              description = ''
                How the service starts:
                - `restore-and-serve`      – load a pre-built graph.bin, then serve (fast).
                - `build-and-serve`        – ingest OSM + GTFS from scratch, then serve
                                             (runs the full build on every restart).
                - `update-gtfs-and-serve`  – reload osm.bin, re-ingest GTFS only, then serve
                                             (faster than a full rebuild when only transit data
                                             changed; requires osm.bin from a prior
                                             `--build --save` run).
              '';
            };

            openFirewall = lib.mkOption {
              type = lib.types.bool;
              default = false;
              description = "Open `settings.server.port` in the NixOS firewall.";
            };

            settings = lib.mkOption {
              description = ''
                Structured config.yaml content. All keys are written verbatim to
                the generated config file; see the upstream config.yaml for
                field-level documentation.
              '';
              default = { };
              type = lib.types.submodule {
                # Allow any extra keys the user wants to pass through.
                freeformType = yamlFormat.type;

                options = {

                  log_level = lib.mkOption {
                    type = lib.types.enum [ "trace" "debug" "info" "warn" "error" ];
                    default = "info";
                    description = "Minimum log level emitted by the routing engine.";
                  };

                  # ── default_routing ────────────────────────────────────────
                  default_routing = lib.mkOption {
                    default = { };
                    description = "Default routing parameters applied to every query.";
                    type = lib.types.submodule {
                      options = {
                        walking_speed = lib.mkOption {
                          type = lib.types.ints.positive;
                          default = 1390;
                          description = "Walking speed in mm/s (1390 ≈ 5 km/h).";
                        };
                        estimator_speed = lib.mkOption {
                          type = lib.types.ints.positive;
                          default = 13900;
                          description = "A* heuristic speed in mm/s (13900 ≈ 50 km/h).";
                        };
                        min_access_secs = lib.mkOption {
                          type = lib.types.nullOr lib.types.ints.positive;
                          default = null;
                          description = ''
                            Walk-radius in seconds used for RAPTOR access/egress stop search.
                            Null uses the compiled-in default (600 s = 10 min).
                          '';
                        };
                      };
                    };
                  };

                  # ── build ─────────────────────────────────────────────────
                  build = lib.mkOption {
                    default = { };
                    description = "Graph ingestion and output paths.";
                    type = lib.types.submodule {
                      options = {
                        inputs = lib.mkOption {
                          type = lib.types.listOf lib.types.attrs;
                          default = [ ];
                          description = ''
                            Ordered list of data sources to ingest.  Each entry must have an
                            `ingestor` key.  Supported ingestors:

                              osm/pbf        – OpenStreetMap PBF file.
                                               Required: url
                              gtfs/generic   – Generic GTFS zip.
                                               Required: name, url
                              gtfs/stib      – STIB-flavoured GTFS zip.
                                               Required: name, url
                              gtfs/sncb      – SNCB-flavoured GTFS zip with a separate OSM file
                                               for railway matching.
                                               Required: name, url, osm_url

                            URLs may use the `path:` scheme (relative to dataDir) or `http(s)://`.
                          '';
                          example = lib.literalExpression ''
                            [
                              {
                                ingestor = "osm/pbf";
                                url      = "path:data/region.osm.pbf";
                              }
                              {
                                ingestor = "gtfs/generic";
                                name     = "MyAgency";
                                url      = "path:data/gtfs.zip";
                              }
                              {
                                ingestor = "gtfs/sncb";
                                name     = "SNCB";
                                url      = "path:data/sncb.zip";
                                osm_url  = "path:data/region.osm.pbf";
                              }
                            ]
                          '';
                        };

                        output = lib.mkOption {
                          type = lib.types.str;
                          default = "graph.bin";
                          description = ''
                            Path for the serialized combined graph (OSM + GTFS).
                            Relative paths are resolved from `dataDir`.
                          '';
                        };

                        osm_output = lib.mkOption {
                          type = lib.types.str;
                          default = "osm.bin";
                          description = ''
                            Path for the OSM-only intermediate graph, used by
                            `update-gtfs-and-serve` to skip the slow OSM ingestion phase.
                            Relative paths are resolved from `dataDir`.
                          '';
                        };

                        delay_models = lib.mkOption {
                          type = lib.types.listOf lib.types.attrs;
                          default = [ ];
                          description = ''
                            Per-mode delay CDF models.  Each entry specifies a transit mode
                            and a list of [delay_seconds, cumulative_probability] breakpoints.
                            Supported modes: subway, metro, tram, bus, rail, train, ferry,
                                             cable_car, cablecar, gondola, funicular.
                          '';
                          example = lib.literalExpression ''
                            [
                              {
                                mode = "bus";
                                bins = [
                                  [-300 0.03] [-120 0.09] [-60 0.16]
                                  [0 0.45] [60 0.58] [120 0.67]
                                  [300 0.84] [600 0.93] [900 0.97] [1800 1.00]
                                ];
                              }
                              {
                                mode = "tram";
                                bins = [
                                  [-300 0.02] [-60 0.15]
                                  [0 0.55] [60 0.67]
                                  [300 0.90] [900 0.98] [1800 1.00]
                                ];
                              }
                            ]
                          '';
                        };
                      };
                    };
                  };

                  # ── server ────────────────────────────────────────────────
                  # NOTE: host and port are not yet read by the binary — the
                  # server currently binds to 0.0.0.0:3000 unconditionally.
                  # These options are defined here so that:
                  #   a) `openFirewall` can open the right port today, and
                  #   b) downstream configs are forward-compatible once the
                  #      binary starts honouring config.yaml's server section.
                  server = lib.mkOption {
                    default = { };
                    description = ''
                      GraphQL server bind settings.
                      Warning: the binary currently ignores these values and
                      always binds to 0.0.0.0:3000.  They are persisted in
                      config.yaml for forward compatibility.
                    '';
                    type = lib.types.submodule {
                      options = {
                        host = lib.mkOption {
                          type = lib.types.str;
                          default = "0.0.0.0";
                          description = "Bind host (not yet read by the binary).";
                        };
                        port = lib.mkOption {
                          type = lib.types.port;
                          default = 3000;
                          description = ''
                            Listen port.  Used by `openFirewall`; the binary
                            currently binds unconditionally to port 3000.
                          '';
                        };
                      };
                    };
                  };
                };
              };
            };
          };

          # ── Implementation ───────────────────────────────────────────────────
          config = lib.mkIf cfg.enable {

            users.users.maas-rs = {
              isSystemUser = true;
              group = "maas-rs";
              home = cfg.dataDir;
              createHome = true;
              description = "maas-rs routing engine service user";
            };
            users.groups.maas-rs = { };

            networking.firewall.allowedTCPPorts =
              lib.mkIf cfg.openFirewall [ cfg.settings.server.port ];

            systemd.services.maas-rs = {
              description = "maas-rs multi-modal routing engine";
              documentation = [ "https://github.com/bdubois/maas-rs" ];
              wantedBy = [ "multi-user.target" ];
              after = [ "network.target" ];

              serviceConfig = {
                Type = "simple";
                User = "maas-rs";
                Group = "maas-rs";
                WorkingDirectory = cfg.dataDir;

                # Deploy the generated config.yaml into the data directory.
                # The `!` prefix runs this step as root so we can write into
                # a directory that may not yet be writable by the service user
                # (e.g. on first boot before createHome has run).
                ExecStartPre = lib.escapeShellArgs [
                  "${pkgs.coreutils}/bin/cp"
                  "--no-preserve=all"
                  "${configFile}"
                  "${cfg.dataDir}/config.yaml"
                ];

                ExecStart = "${cfg.package}/bin/maas-rs ${startArgs}";

                Restart = "on-failure";
                RestartSec = "5s";

                # ── Hardening ──────────────────────────────────────────────
                NoNewPrivileges = true;
                PrivateTmp = true;
                ProtectSystem = "strict";
                ProtectHome = true;
                # The data directory must be writable (graph.bin, osm.bin,
                # downloaded data files, and the generated config.yaml).
                ReadWritePaths = [ cfg.dataDir ];
                ProtectKernelTunables = true;
                ProtectKernelModules = true;
                ProtectControlGroups = true;
                RestrictAddressFamilies = [ "AF_INET" "AF_INET6" ];
                RestrictNamespaces = true;
                LockPersonality = true;
                MemoryDenyWriteExecute = false; # Rust async runtimes need this
              };
            };
          };
        };

      # Compatibility alias for consumers that do
      # `imports = [ maas-rs.nixosModule ]` (without the `s`).
      nixosModule = self.nixosModules.default;
    };
}
