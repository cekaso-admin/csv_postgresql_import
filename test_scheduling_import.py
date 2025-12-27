#!/usr/bin/env python
"""Quick import test for scheduling feature implementation."""

import sys

def test_imports():
    """Test that all new modules import correctly."""
    print("Testing imports...")
    
    try:
        from src.db.schedules import (
            ScheduleRecord,
            create_schedule,
            get_schedule,
            list_schedules,
            update_schedule,
            delete_schedule,
        )
        print("✓ src.db.schedules imports successfully")
    except ImportError as e:
        print(f"✗ Failed to import src.db.schedules: {e}")
        return False
    
    try:
        from src.api.schedule_schemas import (
            ScheduleCreate,
            ScheduleUpdate,
            ScheduleResponse,
            ScheduleListResponse,
        )
        print("✓ src.api.schedule_schemas imports successfully")
    except ImportError as e:
        print(f"✗ Failed to import src.api.schedule_schemas: {e}")
        return False
    
    try:
        from src.api.schedule_routes import schedules_router
        print("✓ src.api.schedule_routes imports successfully")
    except ImportError as e:
        print(f"✗ Failed to import src.api.schedule_routes: {e}")
        return False
    
    try:
        from src.services.scheduler import (
            SchedulerService,
            get_scheduler_service,
            execute_scheduled_import,
        )
        print("✓ src.services.scheduler imports successfully")
    except ImportError as e:
        print(f"✗ Failed to import src.services.scheduler: {e}")
        return False
    
    try:
        from src.main import app
        print("✓ src.main imports successfully (includes scheduler)")
    except ImportError as e:
        print(f"✗ Failed to import src.main: {e}")
        return False
    
    print("\n✓ All imports successful!")
    return True

if __name__ == "__main__":
    success = test_imports()
    sys.exit(0 if success else 1)
