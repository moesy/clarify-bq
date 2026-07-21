use clarify_bq_client::ObjectSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    Records,
    Schemas,
    Lists,
    ListRows,
    Users,
    Workflows,
    Settings,
    Activities,
    Attachments,
}

impl Category {
    pub const ALL: [Category; 9] = [
        Category::Records,
        Category::Schemas,
        Category::Lists,
        Category::ListRows,
        Category::Users,
        Category::Workflows,
        Category::Settings,
        Category::Activities,
        Category::Attachments,
    ];

    pub fn name(&self) -> &'static str {
        match self {
            Category::Records => "records",
            Category::Schemas => "schemas",
            Category::Lists => "lists",
            Category::ListRows => "list_rows",
            Category::Users => "users",
            Category::Workflows => "workflows",
            Category::Settings => "settings",
            Category::Activities => "activities",
            Category::Attachments => "attachments",
        }
    }
}

#[derive(Debug)]
pub struct ResourcePlan {
    pub objects: Vec<ObjectSchema>,
    pub categories: Vec<Category>,
}

impl ResourcePlan {
    /// Build from ALL discovered schemas: value/type schemas are dropped, and
    /// objects appearing under several schema URLs (core/ and entities/) are
    /// deduped by slug, preferring the entities/ document.
    pub fn build(
        schemas: &[ObjectSchema],
        objects: &[String],
        skip: &[String],
    ) -> Result<ResourcePlan, String> {
        let mut by_slug: Vec<ObjectSchema> = Vec::new();
        for s in schemas.iter().filter(|s| s.object && !s.slug.is_empty()) {
            match by_slug.iter_mut().find(|e| e.slug == s.slug) {
                None => by_slug.push(s.clone()),
                Some(existing) => {
                    let prefer_new = s.raw["id"]
                        .as_str()
                        .is_some_and(|id| id.contains("/entities/"));
                    if prefer_new {
                        *existing = s.clone();
                    }
                }
            }
        }
        let schemas = &by_slug;
        let known: Vec<&str> = schemas.iter().map(|s| s.slug.as_str()).collect();
        for o in objects {
            if !known.contains(&o.as_str()) {
                return Err(format!(
                    "unknown object {o:?}; discovered objects: {known:?}"
                ));
            }
        }
        let mut skipped_cats = Vec::new();
        let mut skipped_objects = Vec::new();
        for token in skip {
            if let Some(slug) = token.strip_prefix("records:") {
                if !known.contains(&slug) {
                    return Err(format!(
                        "unknown object in skip token {token:?}; discovered: {known:?}"
                    ));
                }
                skipped_objects.push(slug.to_string());
            } else if let Some(cat) = Category::ALL.iter().find(|c| c.name() == token) {
                skipped_cats.push(*cat);
            } else {
                let vocab: Vec<&str> = Category::ALL.iter().map(|c| c.name()).collect();
                return Err(format!(
                    "unknown skip token {token:?}; valid: {vocab:?} or records:<object>"
                ));
            }
        }
        let objects_out: Vec<ObjectSchema> = schemas
            .iter()
            .filter(|s| objects.is_empty() || objects.contains(&s.slug))
            .filter(|s| !skipped_objects.contains(&s.slug))
            .cloned()
            .collect();
        let categories: Vec<Category> = Category::ALL
            .iter()
            .filter(|c| !skipped_cats.contains(c))
            .copied()
            .collect();
        Ok(ResourcePlan {
            objects: objects_out,
            categories,
        })
    }

    pub fn includes(&self, c: Category) -> bool {
        self.categories.contains(&c)
    }

    pub fn describe(&self) -> String {
        format!(
            "objects: [{}]; categories: [{}]",
            self.objects
                .iter()
                .map(|o| o.slug.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            self.categories
                .iter()
                .map(|c| c.name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schemas() -> Vec<ObjectSchema> {
        ["person", "deal"]
            .iter()
            .map(|s| ObjectSchema {
                slug: s.to_string(),
                relationships: vec![],
                object: true,
                raw: serde_json::json!({}),
            })
            .collect()
    }

    #[test]
    fn dedups_core_and_entities_schemas_preferring_entities() {
        let core = ObjectSchema {
            slug: "person".into(),
            relationships: vec!["company_id".into()],
            object: true,
            raw: serde_json::json!({"id": "https://x/schemas/core/person"}),
        };
        let entities = ObjectSchema {
            slug: "person".into(),
            relationships: vec!["company_id".into(), "deals".into()],
            object: true,
            raw: serde_json::json!({"id": "https://x/schemas/entities/person"}),
        };
        let value_schema = ObjectSchema {
            slug: "https://x/schemas/core/collectionOfStrings".into(),
            relationships: vec![],
            object: false,
            raw: serde_json::json!({}),
        };
        let p = ResourcePlan::build(&[core, entities, value_schema], &[], &[]).unwrap();
        assert_eq!(
            p.objects.len(),
            1,
            "value schemas dropped, duplicates merged"
        );
        assert_eq!(p.objects[0].slug, "person");
        assert_eq!(
            p.objects[0].relationships.len(),
            2,
            "entities/ doc preferred"
        );
    }

    #[test]
    fn default_plan_includes_everything() {
        let p = ResourcePlan::build(&schemas(), &[], &[]).unwrap();
        assert_eq!(p.objects.len(), 2);
        assert_eq!(p.categories.len(), Category::ALL.len());
    }

    #[test]
    fn skip_category_and_per_object() {
        let p = ResourcePlan::build(
            &schemas(),
            &[],
            &["activities".into(), "records:deal".into()],
        )
        .unwrap();
        assert!(!p.includes(Category::Activities));
        assert_eq!(
            p.objects
                .iter()
                .map(|o| o.slug.as_str())
                .collect::<Vec<_>>(),
            vec!["person"]
        );
    }

    #[test]
    fn unknown_skip_token_is_error() {
        let err = ResourcePlan::build(&schemas(), &[], &["workflow".into()]).unwrap_err();
        assert!(err.contains("workflow"));
        assert!(err.contains("workflows"));
    }

    #[test]
    fn unknown_object_is_error() {
        assert!(ResourcePlan::build(&schemas(), &["ghost".into()], &[]).is_err());
    }
}
