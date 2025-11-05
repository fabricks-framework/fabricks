import logging

import fabricks.context.config as c
import fabricks.context.runtime as r


def pprint_runtime(extended: bool = False) -> None:
    print("=" * 60)
    print("FABRICKS RUNTIME CONFIGURATION")
    print("=" * 60)

    # Core Paths Section
    print("\n📁 CORE CONFIG:")
    print(f"   Runtime: {c.PATH_RUNTIME.string}")
    print(f"   Notebooks: {c.PATH_NOTEBOOKS.string}")
    print(f"   Config: {c.PATH_CONFIG.string}")
    print(f"   Log Level: {logging.getLevelName(c.LOGLEVEL)}")
    print(f"   Debug Mode: {'✓' if c.IS_DEBUGMODE else '✗'}")
    print(f"   Job Config from YAML: {'✓' if c.IS_JOB_CONFIG_FROM_YAML else '✗'}")

    print("\n⚙️ RUNTIME SETTINGS:")
    print("\n🔄 PIPELINE STEPS:")

    def _print_steps(steps_list, layer_name, icon):
        if steps_list and any(step for step in steps_list if step):
            print(f"   {icon} {layer_name}:")
            for step in steps_list:
                if step:
                    step_name = step.get("name", "Unnamed")
                    print(f"      • {step_name}")
        else:
            print(f"   {icon} {layer_name}: No steps")

    _print_steps(r.BRONZE, "Bronze", "🥉")
    _print_steps(r.SILVER, "Silver", "🥈")
    _print_steps(r.GOLD, "Gold", "🥇")

    # Storage Configuration Section
    print("\n💾 STORAGE CONFIGURATION:")
    print(f"   Storage URI: {r.FABRICKS_STORAGE.string}")
    print(f"   Storage Credential: {r.FABRICKS_STORAGE_CREDENTIAL or 'Not configured'}")

    # Unity Catalog Section
    print("\n🏛️ UNITY CATALOG:")
    print(f"   Enabled:  {'✓' if r.IS_UNITY_CATALOG else '✗'}")
    if r.IS_UNITY_CATALOG and r.CATALOG:
        print(f"   Catalog: {r.CATALOG}")

    # Security Section
    print("\n🔐 SECURITY:")
    print(f"   Secret Scope: {r.SECRET_SCOPE}")

    print("\n🌐 ADDITIONAL SETTINGS:")
    print(f"   Timezone: {r.TIMEZONE}")

    if extended:
        # Component Paths Section
        print("\n🛠️ COMPONENT PATHS:")
        components = [
            ("UDFs", r.PATH_UDFS),
            ("Parsers", r.PATH_PARSERS),
            ("Extenders", r.PATH_EXTENDERS),
            ("Views", r.PATH_VIEWS),
            ("Schedules", r.PATH_SCHEDULES),
        ]

        for name, path in components:
            print(f"   {name}: {path.string}")

        # Storage Paths Section
        print("\n📦 STORAGE PATHS:")
        for name, path in sorted(r.PATHS_STORAGE.items()):
            icon = "🏭" if name == "fabricks" else "📊"
            print(f"   {icon} {name}: {path.string}")

        # Runtime Paths Section
        if r.PATHS_RUNTIME:
            print("\n⚡ RUNTIME PATHS:")
            for name, path in sorted(r.PATHS_RUNTIME.items()):
                print(f"   📂 {name}: {path.string}")
