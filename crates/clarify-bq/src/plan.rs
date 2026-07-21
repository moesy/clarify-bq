use clarify_client::ObjectSchema;

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
    pub fn build(
        schemas: &[ObjectSchema],
        objects: &[String],
        skip: &[String],
    ) -> Result<ResourcePlan, String> {
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
                raw: serde_json::json!({}),
            })
            .collect()
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
