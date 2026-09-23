use crate::core::{Position, Stance, ThoughtContext};

pub fn opposing_position<'a>(context: &'a ThoughtContext) -> Option<&'a Position> {
    let content = context.observation.content.to_ascii_lowercase();
    context.positions.iter().find(|position| {
        if !matches!(position.stance, Stance::Oppose) {
            return false;
        }
        let subject = position.subject.to_ascii_lowercase();
        let destructive_subject = ["delet", "remov", "destroy", "overwrit"]
            .iter()
            .any(|word| subject.contains(word));
        let destructive_request = ["delet", "remov", "destroy", "overwrit"]
            .iter()
            .any(|word| content.contains(word));
        content.contains(&subject) || (destructive_request && destructive_subject)
    })
}
