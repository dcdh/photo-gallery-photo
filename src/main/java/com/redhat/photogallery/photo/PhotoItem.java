package com.redhat.photogallery.photo;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class PhotoItem extends PanacheEntityBase {

    @Id
    public Long id;
    public String name;
    public String category;

    @Override
    public String toString() {
        return "PhotoItem [id=" + id + ", name=" + name + ", category=" + category + "]";
    }

}
