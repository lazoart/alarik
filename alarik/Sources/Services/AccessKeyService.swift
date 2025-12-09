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
import Vapor
import XMLCoder

struct AccessKeyService {
    static func delete(
        on database: any Database,
        accessKey: String
    )
        async throws
    {
        try await AccessKey.query(on: database)
            .filter(\.$accessKey == accessKey)
            .delete()

        // Remove from all caches
        await AccessKeySecretKeyMapCache.shared.remove(accessKey: accessKey)
        await AccessKeyUserMapCache.shared.remove(accessKey: accessKey)
        await AccessKeyBucketMapCache.shared.removeAccessKey(accessKey)
    }
}
