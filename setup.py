"""
setup.py - Package configuration for VastDB Feast Feature Store

This setup file properly configures the VastDB Feast implementation
as a Python package with appropriate entry points for Feast plugin discovery.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh.readlines() if line.strip() and not line.startswith("#")]

setup(
    name="vastdb-feast-store",
    version="0.1.0",
    author="VastDB Team", 
    author_email="support@vastdata.com",
    description="VastDB-backed Feast feature store implementation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vastdata/vastdb-feast-store",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Database",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "flake8>=4.0.0",
            "mypy>=0.991",
        ],
        "docs": [
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=1.0.0", 
            "sphinxcontrib-napoleon>=0.7",
        ],
    },
    # Feast plugin entry points - this is key for automatic discovery
    entry_points={
        "feast.online_stores": [
            "vastdb_online = vastdb_feast_store:VastdbOnlineStore",
        ],
        "feast.offline_stores": [
            "vastdb_offline = vastdb_feast_store:VastdbOfflineStore",
        ],
        "feast.registries": [
            "vastdb_registry = vastdb_feast_store:VastdbRegistry", 
        ],
        "feast.data_sources": [
            "vastdb_source = vastdb_feast_store:VastdbSource",
        ],
    },
    package_data={
        "vastdb_feast_store": ["*.yaml", "*.yml", "*.json"],
    },
    include_package_data=True,
    zip_safe=False,
)

# Additional configuration for development
if __name__ == "__main__":
    import sys
    import subprocess
    
    if len(sys.argv) > 1 and sys.argv[1] == "develop":
        # Install in development mode
        subprocess.run([
            sys.executable, "-m", "pip", "install", "-e", ".",
        ])
        
        # Also install pre-commit hooks if available
        try:
            subprocess.run(["pre-commit", "install"], check=True)
            print("✅ Pre-commit hooks installed")
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("⚠️  Pre-commit not available - skipping hook installation")
    
    elif len(sys.argv) > 1 and sys.argv[1] == "test":
        # Run tests
        subprocess.run([
            sys.executable, "-m", "pytest", "tests/", "-v", "--cov=vastdb_feast_store"
        ])
    
    elif len(sys.argv) > 1 and sys.argv[1] == "format":
        # Format code
        subprocess.run(["black", "vastdb_feast_store/"])
        subprocess.run(["isort", "vastdb_feast_store/"])
        
    elif len(sys.argv) > 1 and sys.argv[1] == "lint":
        # Lint code
        subprocess.run(["flake8", "vastdb_feast_store/"])
        subprocess.run(["mypy", "vastdb_feast_store/"])
        
    else:
        # Standard setuptools behavior
        from setuptools import setup
        setup()


# Verification script to check if entry points are properly registered
def verify_entry_points():
    """Verify that Feast can discover our custom stores."""
    import pkg_resources
    
    print("Checking Feast entry points...")
    
    # Check online stores
    online_stores = {}
    for ep in pkg_resources.iter_entry_points('feast.online_stores'):
        online_stores[ep.name] = ep.load()
    print(f"Online stores: {list(online_stores.keys())}")
    
    # Check offline stores  
    offline_stores = {}
    for ep in pkg_resources.iter_entry_points('feast.offline_stores'):
        offline_stores[ep.name] = ep.load()
    print(f"Offline stores: {list(offline_stores.keys())}")
    
    # Check registries
    registries = {}
    for ep in pkg_resources.iter_entry_points('feast.registries'):
        registries[ep.name] = ep.load()
    print(f"Registries: {list(registries.keys())}")
    
    # Verify our stores are present
    expected_stores = {
        'vastdb_online': online_stores,
        'vastdb_offline': offline_stores,
        'vastdb_registry': registries,
    }
    
    all_found = True
    for store_name, store_dict in expected_stores.items():
        if store_name not in store_dict:
            print(f"❌ {store_name} not found in entry points")
            all_found = False
        else:
            print(f"✅ {store_name} found and loadable")
    
    if all_found:
        print("🎉 All VastDB stores are properly registered with Feast!")
    else:
        print("⚠️  Some stores are missing - check your installation")
    
    return all_found


# Test configuration to validate the setup
def test_configuration():
    """Test the VastDB Feast configuration."""
    try:
        from vastdb_feast_store import (
            VastdbOnlineStore,
            VastdbOfflineStore, 
            VastdbRegistry,
            VastdbSource
        )
        
        print("✅ All VastDB Feast components imported successfully")
        
        # Test configuration classes
        from vastdb_feast_store import (
            VastdbOnlineStoreConfig,
            VastdbOfflineStoreConfig,
            VastdbRegistryConfig
        )
        
        # Create test configurations
        online_config = VastdbOnlineStoreConfig()
        offline_config = VastdbOfflineStoreConfig()
        registry_config = VastdbRegistryConfig()
        
        print("✅ All configuration classes created successfully")
        print(f"Online store type: {online_config.type}")
        print(f"Offline store type: {offline_config.type}")
        print(f"Registry type: {registry_config.type}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False


if __name__ == "__main__":
    # Run verification if called directly
    print("VastDB Feast Store Setup Verification")
    print("=" * 40)
    
    config_ok = test_configuration()
    entry_points_ok = verify_entry_points()
    
    if config_ok and entry_points_ok:
        print("\n🎉 VastDB Feast Store setup completed successfully!")
        print("You can now use VastDB as your Feast backend.")
    else:
        print("\n⚠️  Setup verification failed. Please check the installation.")
