# Knowledge Base

## Go Build & Distribution Strategy

### 1. Binary Management
**Do not commit binaries to Git.**
- **Reason**: Go binaries are compiled for a specific Operating System (Linux, Windows, macOS) and Architecture (amd64, arm64). A binary built on your machine likely won't run on another user's machine.
- **Action**: Add the output binary name (e.g., `dqc`) to `.gitignore`.

### 2. Build Commands
Go provides two standard ways for users to build your application from source:

- **`go build`**
    - **What it does**: Compiles the code and creates a binary in the **current directory**.
    - **Use Case**: Testing, development, or creating a binary to ship manually.
    - **Command**: `go build -o dqc ./cmd/dqc`

- **`go install`**
    - **What it does**: Compiles the code and places the binary in `$GOPATH/bin` (system path).
    - **Use Case**: End-user installation. Allows running the tool globally from any terminal window.
    - **Command**: `go install github.com/user/repo/cmd/dqc@latest`

### 3. Binary Size & Static Linking
- **Observation**: Binaries can be large (>50MB).
- **Cause**: Libraries like **DuckDB** are *statically linked*. This means the entire C++ library is embedded inside the Go binary.
- **Benefit**: The binary is self-contained. The user does not need to install DuckDB separately; it "just works."

### 4. Development Philosophy & Rules

**Agents must follow this Layered Architecture approach for all new features:**

1.  **Phase 1: Core Domain Logic (`internal/`)**
    *   **Rule**: Always implement business logic and data access here first.
    *   **Constraint**: This code must remain "pure" — it must NOT depend on `cmd/`, `api/`, or any external interface packages.
    *   **Goal**: Ensure the core logic is testable and reusable by any interface (CLI, API, etc.).

2.  **Phase 2: Interface Layer (`cmd/`)**
    *   **Rule**: Build the CLI/API only *after* the core logic is stable.
    *   **Constraint**: The `cmd` package is a *consumer*. It should only parse user input (flags, args) and call `internal/`. It should contain minimal logic.
    *   **Goal**: Keep entry points lightweight and swappable.
