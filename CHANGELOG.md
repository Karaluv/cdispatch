# Changelog

## 0.1.0 — initial portable extraction

- Package the existing queue, planner, lifecycle, Slurm and observer code.
- Separate scientific adapters into an optional companion package.
- Add explicit versioned plugin hooks and external config/state/profile paths.
- Add offline configuration initialization and validation commands.
- Remove scientific sidecar reads and site-specific node inference from core.
- Decouple regression tests from the changing production configuration.

The extraction has offline regression coverage. Live upgrade validation is a
separate deployment step; see docs/migration.md.
