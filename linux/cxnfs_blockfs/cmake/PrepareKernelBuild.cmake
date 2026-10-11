if(NOT CXNFS_KERNEL_RELEASE)
	message(FATAL_ERROR "CXNFS_KERNEL_RELEASE is required")
endif()

if(NOT CXNFS_KERNEL_BUILD_DIR)
	message(FATAL_ERROR "CXNFS_KERNEL_BUILD_DIR is required")
endif()

string(REPLACE "\"" "" CXNFS_KERNEL_RELEASE "${CXNFS_KERNEL_RELEASE}")
string(REPLACE "\"" "" CXNFS_KERNEL_BUILD_DIR "${CXNFS_KERNEL_BUILD_DIR}")
string(REPLACE "\"" "" CXNFS_DOWNLOAD_DEPS "${CXNFS_DOWNLOAD_DEPS}")
string(REPLACE "\"" "" CXNFS_HOST_SYSTEM_NAME_CLEAN "${CXNFS_HOST_SYSTEM_NAME}")

if("${CXNFS_HOST_SYSTEM_NAME_CLEAN}" STREQUAL "Linux")
	# Continue with Linux build-environment checks below.
else()
	message(WARNING "Host system is ${CXNFS_HOST_SYSTEM_NAME_CLEAN}; kernel module builds require the Linux VM.")
	return()
endif()

find_program(CXNFS_MAKE make)
find_program(CXNFS_CC cc)

set(CXNFS_NEEDS_DEPS 0)
if(NOT CXNFS_MAKE)
	set(CXNFS_NEEDS_DEPS 1)
endif()
if(NOT CXNFS_CC)
	set(CXNFS_NEEDS_DEPS 1)
endif()
if(NOT EXISTS "${CXNFS_KERNEL_BUILD_DIR}/Makefile")
	set(CXNFS_NEEDS_DEPS 1)
endif()

if(CXNFS_NEEDS_DEPS EQUAL 0)
	message(STATUS "Kernel build directory found: ${CXNFS_KERNEL_BUILD_DIR}")
	return()
endif()

if("${CXNFS_DOWNLOAD_DEPS}" STREQUAL "1")
	find_program(CXNFS_APT_GET apt-get)
	if(NOT CXNFS_APT_GET)
		message(FATAL_ERROR "Missing build dependencies and apt-get was not found. Expected kernel build dir: ${CXNFS_KERNEL_BUILD_DIR}")
	endif()

	message(STATUS "Installing kernel build dependencies for ${CXNFS_KERNEL_RELEASE}")
	execute_process(
		COMMAND sudo apt-get update
		RESULT_VARIABLE apt_update_result
	)
	if(NOT apt_update_result EQUAL 0)
		message(FATAL_ERROR "apt-get update failed")
	endif()

	execute_process(
		COMMAND sudo apt-get install -y build-essential "linux-headers-${CXNFS_KERNEL_RELEASE}"
		RESULT_VARIABLE apt_install_result
	)
	if(NOT apt_install_result EQUAL 0)
		message(FATAL_ERROR "apt-get install failed")
	endif()

	find_program(CXNFS_MAKE_AFTER make)
	if(NOT CXNFS_MAKE_AFTER)
		message(FATAL_ERROR "make still was not found after installing build-essential")
	endif()

	find_program(CXNFS_CC_AFTER cc)
	if(NOT CXNFS_CC_AFTER)
		message(FATAL_ERROR "cc still was not found after installing build-essential")
	endif()

	if(EXISTS "${CXNFS_KERNEL_BUILD_DIR}/Makefile")
		message(STATUS "Kernel build directory ready: ${CXNFS_KERNEL_BUILD_DIR}")
		return()
	endif()
endif()

message(FATAL_ERROR
	"Linux kernel build directory not found: ${CXNFS_KERNEL_BUILD_DIR}\n"
	"Install dependencies on Ubuntu with:\n"
	"  sudo apt-get update\n"
	"  sudo apt-get install -y build-essential linux-headers-${CXNFS_KERNEL_RELEASE}\n"
	"Or configure CMake with -DCXNFS_DOWNLOAD_DEPS=ON."
)
