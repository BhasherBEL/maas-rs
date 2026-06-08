{
  description = "maas-rs — multi-modal MaaS routing engine (A* + RAPTOR)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    let
      # Standalone package build — reused by eachDefaultSystem and the NixOS module.
      mkPackage =
        pkgs:
        pkgs.rustPlatform.buildRustPackage {
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
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages.default = mkPackage pkgs;

        devShells.default = pkgs.mkShell {
          # Inherit all build inputs from the package.
          inputsFrom = [ self.packages.${system}.default ];
          packages = with pkgs; [
            rust-analyzer
            rustfmt
            clippy
            python3
          ];
          # Replicate .envrc: expose openssl for cargo build outside nix.
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
        };
      }
    )

    # ── System-independent outputs (NixOS module) ─────────────────────────────
    // {
      nixosModules.default =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.maas-rs;
          yamlFormat = pkgs.formats.yaml { };

          configFile = yamlFormat.generate "maas-rs.yaml" cfg.settings;

          startArgs =
            {
              "restore-and-serve" = "--restore --serve";
              "build-and-serve" = "--build --serve";
              "update-gtfs-and-serve" = "--update-gtfs --serve";
            }
            .${cfg.mode};
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
              type = yamlFormat.type;
              default = { };
              description = ''
                Verbatim `config.yaml` content. The whole attrset is serialized
                to YAML as-is and copied to `''${dataDir}/config.yaml` at startup,
                so every field the binary understands is supported automatically
                — no per-key plumbing in this module. See the upstream
                `config.yaml` for the full, authoritative schema (`build.inputs`,
                `build.delay_models`, `default_routing`, `server`, `auto_update`,
                `realtime`, `log_level`).

                Only one key is interpreted by this module: `settings.server.port`,
                read by `openFirewall` (falls back to 3000 when unset).

                URLs in `build.inputs` may use `path:` (relative to `dataDir`) or
                `http(s)://`. Keep secrets out of the Nix store: reference them via
                `''${VAR}` (environment) or `''${file:/path}` placeholders in
                header values rather than inlining them here.
              '';
              example = lib.literalExpression ''
                {
                  log_level = "info";

                  build = {
                    inputs = [
                      { ingestor = "osm/pbf"; url = "path:data/region.osm.pbf"; }
                      { ingestor = "gtfs/generic"; name = "MyAgency"; url = "path:data/gtfs.zip"; }
                      {
                        ingestor = "gtfs/sncb";
                        name     = "SNCB";
                        url      = "https://example.com/sncb/static/";
                        osm_url  = "path:data/region.osm.pbf";
                        headers."bmc-partner-key" = "''${BMC_PARTNER_KEY}";
                      }
                    ];
                    output     = "graph.bin";
                    osm_output = "osm.bin";
                    delay_models = [
                      { mode = "bus";  bins = [ [ (-300) 0.03 ] [ 0 0.45 ] [ 1800 1.00 ] ]; }
                      { mode = "tram"; bins = [ [ (-300) 0.02 ] [ 0 0.55 ] [ 1800 1.00 ] ]; }
                    ];
                  };

                  default_routing = { walking_speed_mps = 1.2; };

                  server = { host = "0.0.0.0"; port = 3000; };

                  auto_update = { enabled = true; schedule = "0 * * * *"; cache_dir = "cache"; };

                  realtime = {
                    enabled = true;
                    poll_interval_secs = 30;
                    feeds = [
                      { type = "gtfs-rt"; name = "sncb"; url = "https://example.com/rt?format=protobuf";
                        headers."bmc-partner-key" = "''${BMC_PARTNER_KEY}"; }
                    ];
                  };
                }
              '';
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

            networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [
              (cfg.settings.server.port or 3000)
            ];

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
                RestrictAddressFamilies = [
                  "AF_INET"
                  "AF_INET6"
                ];
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
