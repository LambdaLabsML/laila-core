# LAILA

LAILA stands for Lambda's Interdisciplinary Large Atlas.

LAILA is a platform for unifying training, simulation, and data management into a single computational workflow. It is built on a simple idea: compute and data are two sides of the same coin. In modern technical systems, models, datasets, simulations, artifacts, and execution environments are often split across disconnected tools and storage layers. LAILA is designed to reduce that fragmentation and provide a more coherent interface for working across them.

At its core, LAILA encapsulates distributed silos of compute and data behind a consistent programming model. The goal is to make massively distributed work feel straightforward, composable, and practical, whether the workload involves machine learning pipelines, simulation outputs, stored artifacts, or hybrid workflows that span multiple backends.

## Vision

LAILA is intended to serve as an interdisciplinary platform for teams that need to move fluidly between data creation, data storage, model training, and large-scale execution. Rather than treating infrastructure boundaries as the primary abstraction, LAILA focuses on ergonomic syntax and reusable interfaces that let users reason about workflows at a higher level.

This approach makes it easier to:

- organize and manage data across multiple storage systems
- connect compute and memory workflows with less boilerplate
- build distributed pipelines that remain readable and maintainable
- reduce the operational friction between experimentation and production-scale execution

## Current Release

LAILA is currently in **beta 1.0**.

The current release includes the **memory module** as the first public component of the broader platform. This release is intended to establish the foundation for LAILA's data and workflow abstractions while additional modules continue to mature.

As a beta release, interfaces may continue to evolve as the platform expands and real-world usage informs the next stage of development.

## Installation

Install the currently released package with:

```bash
pip install laila-core
```

## Project Focus

The long-term focus of LAILA is to provide a common layer for:

- data management across heterogeneous storage backends
- compute workflows that can scale across distributed environments
- training and simulation pipelines that benefit from a shared abstraction layer
- syntax that makes complex infrastructure easier to use without hiding the underlying flexibility

In practice, this means building tools that help users treat datasets, stored objects, memory systems, and execution backends as parts of a single workflow rather than separate systems stitched together manually.

## Credits

- Creator: Amir Zadeh
- Tutorials: Jessica Nicholson
- Acknowledgements: Jason Zhang, Xuweiyi Chen, Connor Alvarez
