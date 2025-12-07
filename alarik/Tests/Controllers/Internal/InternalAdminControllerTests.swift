/*
Copyright 2025-present Julian Gerhards

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import Fluent
import Foundation
import Testing
import Vapor
import VaporTesting

@testable import Alarik

@Suite("InternalAdminController tests", .serialized)
struct InternalAdminControllerTests {
    private func withApp(_ test: (Application) async throws -> Void) async throws {
        let app = try await Application.make(.testing)
        do {
            try StorageHelper.cleanStorage()
            defer { try? StorageHelper.cleanStorage() }
            try await configure(app)
            try await app.autoMigrate()
            try await test(app)
            try await app.autoRevert()
        } catch {
            try? StorageHelper.cleanStorage()
            try? await app.autoRevert()
            try await app.asyncShutdown()
            throw error
        }
        try await app.asyncShutdown()
    }

    @Test("List users as admin - should pass")
    func testListUsers() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            // Create 3 users
            try await createRandomUser(app)
            try await createRandomUser(app)
            try await createRandomUser(app)

            try await app.test(
                .GET, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    let page = try res.content.decode(Page<User.ResponseDTO>.self)
                    #expect(res.status == .ok)
                    #expect(page.items.count == 4)
                })
        }
    }

    @Test("List users as non admin - should fail")
    func testListUsersAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            // Create 3 users
            try await createRandomUser(app)
            try await createRandomUser(app)
            try await createRandomUser(app)

            try await app.test(
                .GET, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })
        }
    }

    @Test("Create user as admin - should pass")
    func testCreateUser() async throws {
        try await withApp { app in
            let token = try await loginDefaultAdminUser(app)

            let createDTO = User.Create(
                name: "John Doe",
                username: "john@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            try await app.test(
                .GET, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .ok)
                })
        }
    }

    @Test("Create user as non admin - should fail")
    func testCreateUserAsNonAdmin() async throws {
        try await withApp { app in
            let token = try await createUserAndLogin(app)

            let createDTO = User.Create(
                name: "John Doe",
                username: "john@example.com",
                password: "SecurePass123!",
                isAdmin: false
            )

            try await app.test(
                .GET, "/api/v1/admin/users",
                beforeRequest: { req in
                    req.headers.bearerAuthorization = BearerAuthorization(token: token)
                    try req.content.encode(createDTO)
                },
                afterResponse: { res async throws in
                    #expect(res.status == .unauthorized)
                })
        }
    }
}
