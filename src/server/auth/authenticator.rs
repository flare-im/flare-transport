//! 认证器 Trait
//!
//! 定义认证接口，允许用户实现自定义的 token 验证逻辑

use crate::common::device::DeviceInfo;
use crate::common::error::Result;
use async_trait::async_trait;

/// 认证元数据与连接元数据中表示租户的键
///
/// 认证器把已验证令牌里的租户写在 [`AuthResult::user_metadata`] 的这个键下
/// （见 [`AuthResult::with_tenant_id`]），服务端据此按（租户，用户）隔离设备冲突，
/// 并把同一个值写进连接元数据，业务层读到的连接租户与设备分桶用的租户始终一致。
pub const TENANT_ID_METADATA_KEY: &str = "tenant_id";

/// 认证结果
#[derive(Debug, Clone)]
pub struct AuthResult {
    /// 是否验证通过
    pub authenticated: bool,
    /// 用户 ID（如果验证通过）
    pub user_id: Option<String>,
    /// 错误消息（如果验证失败）
    pub error_message: Option<String>,
    /// 用户元数据（可选，用于存储额外的用户信息）
    pub user_metadata: Option<std::collections::HashMap<String, String>>,
    /// 拒绝是否只是暂时的（只在 `authenticated == false` 时有意义）
    ///
    /// 认证依赖暂时读不到（例如租户状态库抖动）时，认证器没有对令牌或租户下结论，只是此刻
    /// 无法放行。服务端据此以 `SERVICE_UNAVAILABLE` 拒绝 CONNECT，客户端退避重连即可；
    /// 不能报成鉴权失败或租户不可用，否则客户端会去换令牌或终止会话。见
    /// [`AuthResult::retryable_failure`]。
    pub retryable: bool,
    /// 这条连接所属应用的最新接入清单 serial，随 CONNECT_ACK 下发；0 表示服务端不知道
    pub access_serial: u64,
}

impl AuthResult {
    /// 创建成功的认证结果
    pub fn success(user_id: Option<String>) -> Self {
        Self {
            authenticated: true,
            user_id,
            error_message: None,
            user_metadata: None,
            retryable: false,
            access_serial: 0,
        }
    }

    /// 创建成功的认证结果（带元数据）
    pub fn success_with_metadata(
        user_id: Option<String>,
        metadata: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            authenticated: true,
            user_id,
            error_message: None,
            user_metadata: Some(metadata),
            retryable: false,
            access_serial: 0,
        }
    }

    /// 创建失败的认证结果
    pub fn failure(error_message: String) -> Self {
        Self {
            authenticated: false,
            user_id: None,
            error_message: Some(error_message),
            user_metadata: None,
            retryable: false,
            access_serial: 0,
        }
    }

    /// 创建暂时性的拒绝：认证依赖暂时不可用，稍后重试可能放行
    ///
    /// 服务端以 `SERVICE_UNAVAILABLE` 拒绝 CONNECT（关连接前下发的 ERROR 帧 metadata `code` 也是
    /// 它）。`error_message` 是类型化错误码时原样作为 ERROR 帧的 message，否则下发
    /// `SERVICE_UNAVAILABLE`，文本只进服务端日志。
    pub fn retryable_failure(error_message: String) -> Self {
        Self {
            retryable: true,
            ..Self::failure(error_message)
        }
    }

    /// 写入已验证的租户（记在 [`TENANT_ID_METADATA_KEY`] 下）
    #[must_use]
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.user_metadata
            .get_or_insert_with(Default::default)
            .insert(TENANT_ID_METADATA_KEY.to_string(), tenant_id.into());
        self
    }

    /// 写入这条连接要在 CONNECT_ACK 里看到的接入清单 serial
    #[must_use]
    pub fn with_access_serial(mut self, access_serial: u64) -> Self {
        self.access_serial = access_serial;
        self
    }

    /// 已验证的租户；认证器没有给出时为 `None`
    pub fn tenant_id(&self) -> Option<&str> {
        self.user_metadata
            .as_ref()
            .and_then(|metadata| metadata.get(TENANT_ID_METADATA_KEY))
            .map(String::as_str)
    }
}

/// 认证器 Trait
///
/// 实现此 trait 以提供自定义的 token 验证逻辑
/// 例如：JWT 验证、数据库查询、Redis 验证等
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// 验证 token
    ///
    /// # 参数
    /// - `token`: 客户端提供的 token（从 CONNECT 消息的 metadata 中提取）
    /// - `connection_id`: 连接 ID
    /// - `device_info`: 设备信息（可选，客户端可能提供）
    /// - `metadata`: CONNECT 消息的其他元数据（可选）
    ///
    /// # 返回
    /// - `Ok(AuthResult)`: 认证结果
    /// - `Err`: 认证过程出错（非验证失败，而是系统错误）。服务端与
    ///   [`AuthResult::retryable_failure`] 一样以 `SERVICE_UNAVAILABLE` 拒绝 CONNECT，客户端退避
    ///   重连；对令牌或租户的结论要返回 `Ok(AuthResult::failure(..))`，否则客户端不会换令牌。
    async fn authenticate(
        &self,
        token: &str,
        connection_id: &str,
        device_info: Option<&DeviceInfo>,
        metadata: Option<&std::collections::HashMap<String, Vec<u8>>>,
    ) -> Result<AuthResult>;

    /// 检查连接是否已验证（可选，用于验证状态管理）
    ///
    /// 默认实现返回 true（假设验证通过后连接状态由 ConnectionManager 管理）
    /// 如果需要自定义验证状态管理（如 Redis、数据库），可以覆盖此方法
    async fn is_authenticated(&self, connection_id: &str) -> Result<bool> {
        let _ = connection_id;
        Ok(true)
    }

    /// 使连接失效（可选，用于主动撤销认证）
    ///
    /// 默认实现不做任何操作
    /// 如果需要支持主动撤销认证（如用户登出、token 撤销），可以覆盖此方法
    async fn revoke_authentication(&self, connection_id: &str) -> Result<()> {
        let _ = connection_id;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_retryable_failure_is_marked_transient() {
        let transient = AuthResult::retryable_failure("projection unreachable".to_string());
        assert!(!transient.authenticated);
        assert!(transient.retryable);
        assert_eq!(
            transient.error_message.as_deref(),
            Some("projection unreachable")
        );

        assert!(!AuthResult::failure("TENANT_UNAVAILABLE".to_string()).retryable);
        assert!(!AuthResult::success(Some("u".to_string())).retryable);
        assert!(
            !AuthResult::success_with_metadata(Some("u".to_string()), Default::default()).retryable
        );
    }

    #[test]
    fn tenant_is_carried_in_the_auth_metadata() {
        let result = AuthResult::success(Some("u".to_string()));
        assert_eq!(result.tenant_id(), None);

        let result = result.with_tenant_id("t-1");
        assert_eq!(result.tenant_id(), Some("t-1"));
        assert_eq!(
            result
                .user_metadata
                .as_ref()
                .and_then(|metadata| metadata.get(TENANT_ID_METADATA_KEY))
                .map(String::as_str),
            Some("t-1")
        );

        let with_metadata = AuthResult::success_with_metadata(
            Some("u".to_string()),
            std::collections::HashMap::from([("device_id".to_string(), "d".to_string())]),
        )
        .with_tenant_id("t-2");
        assert_eq!(with_metadata.tenant_id(), Some("t-2"));
        assert_eq!(
            with_metadata
                .user_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("device_id"))
                .map(String::as_str),
            Some("d")
        );
    }
}
