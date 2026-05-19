# mxl-k8s architecture

This directory describes how the moving parts of `mxl-k8s` fit
together at runtime.

## Contents

1. [System context](./01-system-context.md): what's inside the
   cluster, what's external, where the data lives.
2. [Components](./02-components.md): what the operator, gateway,
   agent, and shim each do, and how applying an `MxlReceiver`
   flows through them.
3. [Per-node anatomy](./03-node-anatomy.md): single-node tmpfs
   layout, the shared-mmap zero-copy topology, and how a single
   grain travels through it.

## Editing the diagrams

Every diagram is stored as a `*.drawio.svg` file under
[`diagrams/`](./diagrams/). That's drawio's hybrid format: an SVG
whose root element carries a URL-encoded `mxfile` in its `content`
attribute, and whose body is a rendered preview of that XML.

The XML inside `content="..."` is the source of truth; the SVG body
beneath it is a cache drawio regenerates on save. To edit:

1. Open the file in VS Code with the
   [Draw.io Integration extension](https://marketplace.visualstudio.com/items?itemName=hediet.vscode-drawio),
   or in the drawio desktop app.
2. Make changes on the canvas.
3. Save. The extension rewrites the SVG body so GitHub previews
   the updated diagram inline.
4. Commit the file.

If a diagram looks blank or incomplete on GitHub, open it once in
drawio and save: that re-renders the SVG body from the canonical
XML.
