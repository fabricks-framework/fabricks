import logging

import fabricks.context.config as c
import fabricks.context.runtime as r


def pprint_runtime(extended: bool = True) -> None:
    print("=" * 60)
    print("FABRICKS RUNTIME CONFIGURATION")
    print("=" * 60)

    # Core Paths Section
    print("\n📁 CONFIG:")
    print(f"    • Runtime: {c.PATH_RUNTIME.string}")
    print(f"    • Notebooks: {c.PATH_NOTEBOOKS.string}")
    print(f"    • Config: {c.PATH_CONFIG.string}")
    print(f"    • Log Level: {logging.getLevelName(c.LOGLEVEL)}")
    print(f"    • Debug Mode: {'✅' if c.IS_DEBUGMODE else '❌'}")
    print(f"    • Job Config from YAML: {'✅' if c.IS_JOB_CONFIG_FROM_YAML else '❌'}")

    print("\n⚙️ STEPS:")

    def _print_steps(
        steps: dict[str, r.StepBronzeConf] | dict[str, r.StepSilverConf] | dict[str, r.StepGoldConf], layer, icon
    ):
        if steps:
            print(f"   {icon} {layer}:")
            for step in steps.values():
                print(f"      • {step.name}")
                if extended:
                    print(f"         - 📖 {r.PATHS_RUNTIME.get(step.name)}")
                    print(f"         - 💾 {r.PATHS_STORAGE.get(step.name)}")
        else:
            print(f"   {icon} {layer}: No steps")

    _print_steps(r.BRONZE, "Bronze", "🥉")
    _print_steps(r.SILVER, "Silver", "🥈")
    _print_steps(r.GOLD, "Gold", "🥇")

    # Storage Configuration Section
    print("\n💾 FABRICKS STORAGE:")
    print(f"    • Storage URI: {r.FABRICKS_STORAGE.string}")
    print(f"    • Storage Credential: {r.FABRICKS_STORAGE_CREDENTIAL or 'Not configured'}")

    # Unity Catalog Section
    print("\n🏛️ UNITY CATALOG:")
    print(f"    • Enabled:  {'✅' if r.IS_UNITY_CATALOG else '❌'}")
    if r.IS_UNITY_CATALOG and r.CATALOG:
        print(f"    • Catalog: {r.CATALOG}")

    # Security Section
    print("\n🔐 SECURITY:")
    print(f"    • Secret Scope: {r.SECRET_SCOPE}")
    print("\n🌐 ADDITIONAL SETTINGS:")
    print(f"    • Timezone: {r.TIMEZONE}")

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
            print(f"    • {name}: {path.string}")
