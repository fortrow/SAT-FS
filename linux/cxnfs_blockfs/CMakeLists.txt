cmake_minimum_required(VERSION 3.22)

project(cxnfs_blockfs LANGUAGES NONE)

option(CXNFS_DOWNLOAD_DEPS "Download/install missing build dependencies with apt-get on Linux." ON)
option(CXNFS_INSTALL_DEPS "Deprecated alias for CXNFS_DOWNLOAD_DEPS." OFF)

find_program(CXNFS_MAKE_EXECUTABLE make)
if(NOT CXNFS_MAKE_EXECUTABLE)
	set(CXNFS_MAKE_EXECUTABLE make)
endif()

find_program(CXNFS_CC_EXECUTABLE cc)
if(NOT CXNFS_CC_EXECUTABLE)
	set(CXNFS_CC_EXECUTABLE cc)
endif()

execute_process(
	COMMAND uname -r
	OUTPUT_VARIABLE CXNFS_UNAME_RELEASE
	OUTPUT_STRIP_TRAILING_WHITESPACE
	ERROR_QUIET
)

set(CXNFS_KERNEL_RELEASE "${CXNFS_UNAME_RELEASE}" CACHE STRING "Linux kernel release to build against.")
set(CXNFS_KERNEL_BUILD_DIR "/lib/modules/${CXNFS_KERNEL_RELEASE}/build" CACHE PATH "Linux kernel build directory.")
set(CXNFS_TOOL_OUTPUT_DIR "${CMAKE_BINARY_DIR}/tools")

set(CXNFS_PREPARE_DOWNLOAD_DEPS "${CXNFS_DOWNLOAD_DEPS}")
if(CXNFS_INSTALL_DEPS)
	set(CXNFS_PREPARE_DOWNLOAD_DEPS ON)
endif()

add_custom_target(prepare-build-env
	COMMAND
		"${CMAKE_COMMAND}"
		-DCXNFS_KERNEL_RELEASE=${CXNFS_KERNEL_RELEASE}
		-DCXNFS_KERNEL_BUILD_DIR=${CXNFS_KERNEL_BUILD_DIR}
		-DCXNFS_DOWNLOAD_DEPS="$<IF:$<BOOL:${CXNFS_PREPARE_DOWNLOAD_DEPS}>,1,0>"
		-DCXNFS_HOST_SYSTEM_NAME=${CMAKE_HOST_SYSTEM_NAME}
		-P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PrepareKernelBuild.cmake"
	COMMENT "Checking CXNFS Linux kernel build environment"
	VERBATIM
)

set(CXNFS_TOOL_CFLAGS -std=c11 -Wall -Wextra -Werror -O2)

function(cxnfs_tool target source)
	set(output "${CXNFS_TOOL_OUTPUT_DIR}/${target}")
	add_custom_command(
		OUTPUT "${output}"
		COMMAND "${CMAKE_COMMAND}" -E make_directory "${CXNFS_TOOL_OUTPUT_DIR}"
		COMMAND "${CXNFS_CC_EXECUTABLE}" ${CXNFS_TOOL_CFLAGS} -I"${CMAKE_CURRENT_SOURCE_DIR}" -o "${output}" "${CMAKE_CURRENT_SOURCE_DIR}/${source}"
		DEPENDS
			"${CMAKE_CURRENT_SOURCE_DIR}/${source}"
			${ARGN}
		COMMENT "Building ${target}"
		VERBATIM
	)
	add_custom_target(${target} DEPENDS "${output}")
	add_dependencies(${target} prepare-build-env)
endfunction()

cxnfs_tool(mkfs.cxnfsblk
	tools/mkfs.cxnfsblk.c
	"${CMAKE_CURRENT_SOURCE_DIR}/tools/cxnfs_tool_common.h"
	"${CMAKE_CURRENT_SOURCE_DIR}/cxnfs_blockfs_disk.h"
	"${CMAKE_CURRENT_SOURCE_DIR}/cxnfs_blockfs_meta.h"
)

cxnfs_tool(fsck.cxnfsblk
	tools/fsck.cxnfsblk.c
	"${CMAKE_CURRENT_SOURCE_DIR}/tools/cxnfs_tool_common.h"
	"${CMAKE_CURRENT_SOURCE_DIR}/cxnfs_blockfs_disk.h"
	"${CMAKE_CURRENT_SOURCE_DIR}/cxnfs_blockfs_meta.h"
)

set(CXNFS_TOOL_TARGETS mkfs.cxnfsblk fsck.cxnfsblk)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
	cxnfs_tool(ctl_probe
		tools/ctl_probe.c
		"${CMAKE_CURRENT_SOURCE_DIR}/cxnfs_blockfs_uapi.h"
	)
	cxnfs_tool(cxnfs-blockfs-ctl
		tools/cxnfs-blockfs-ctl.c
		"${CMAKE_CURRENT_SOURCE_DIR}/cxnfs_blockfs_uapi.h"
	)
	list(APPEND CXNFS_TOOL_TARGETS ctl_probe cxnfs-blockfs-ctl)
endif()

add_custom_target(tools DEPENDS ${CXNFS_TOOL_TARGETS})

add_custom_target(tool-smoke
	COMMAND "${CMAKE_COMMAND}" -E env "CXNFS_TOOL_SMOKE_REPORT_DIR=${CMAKE_BINARY_DIR}/tool-smoke-reports" sh "${CMAKE_CURRENT_SOURCE_DIR}/tools/tool-smoke.sh" "${CXNFS_TOOL_OUTPUT_DIR}"
	DEPENDS ${CXNFS_TOOL_TARGETS}
	WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
	COMMENT "Running CXNFS blockfs userspace tool smoke test"
	VERBATIM
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
	add_custom_target(module
		COMMAND "${CXNFS_MAKE_EXECUTABLE}" -C "${CMAKE_CURRENT_SOURCE_DIR}" KDIR="${CXNFS_KERNEL_BUILD_DIR}" all
		DEPENDS prepare-build-env
		WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
		COMMENT "Building cxnfs_blockfs.ko with the Linux kernel build system"
		VERBATIM
	)

	add_custom_target(module-clean
		COMMAND "${CXNFS_MAKE_EXECUTABLE}" -C "${CMAKE_CURRENT_SOURCE_DIR}" KDIR="${CXNFS_KERNEL_BUILD_DIR}" clean
		WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
		COMMENT "Cleaning cxnfs_blockfs kernel build outputs"
		VERBATIM
	)

	add_custom_target(vm-smoke
		COMMAND "${CMAKE_COMMAND}" -E env CXNFS_USE_CMAKE=0 CXNFS_BUILD_FROM_SOURCE=0 "CXNFS_TOOLS_DIR=${CXNFS_TOOL_OUTPUT_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/vm-smoke.sh"
		DEPENDS tools module
		WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
		COMMENT "Running CXNFS blockfs VM smoke test"
		VERBATIM
	)
else()
	add_custom_target(module
		COMMAND "${CMAKE_COMMAND}" -E false
		COMMENT "cxnfs_blockfs.ko can only be built on Linux with matching kernel headers"
		VERBATIM
	)
endif()
